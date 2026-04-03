package com.momao.valkey.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.momao.valkey.adapter.BaseValkeyRepository;
import com.momao.valkey.adapter.ValkeyClientRouting;
import com.momao.valkey.autoconfigure.ValkeyQueryAutoConfiguration;
import com.momao.valkey.core.AggregateResult;
import com.momao.valkey.core.AggregateExpressions;
import com.momao.valkey.core.BulkSaveItem;
import com.momao.valkey.core.BulkUpdateItem;
import com.momao.valkey.core.BulkWriteOptions;
import com.momao.valkey.core.BulkWriteResult;
import com.momao.valkey.core.FacetResult;
import com.momao.valkey.core.NumericUpdateOperation;
import com.momao.valkey.core.UpdateAssignment;
import com.momao.valkey.core.UpdateOperationKind;
import com.momao.valkey.core.ValkeyFieldReference;
import com.momao.valkey.core.Page;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;

import java.util.List;
import java.util.Locale;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@SpringBootTest(classes = ValkeyQueryDemoApplication.class, webEnvironment = SpringBootTest.WebEnvironment.NONE)
@EnabledIfEnvironmentVariable(named = "VALKEY_RW_INTEGRATION", matches = "true")
@TestPropertySource(properties = {
        "valkey.query.enabled=true",
        "valkey.query.mode=read_write_split",
        "valkey.query.read-preference=replica_preferred"
})
class ReadWriteSplitIntegrationTests {

    record SkuProjectionView(String id, String title, String merchantName) {
    }

    @Autowired
    private ValkeyClientRouting clientRouting;

    @Autowired
    private SkuRepository skuRepository;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private ValkeyQueryAutoConfiguration.ValkeyConnectionInfo connectionInfo;

    @DynamicPropertySource
    static void registerProperties(DynamicPropertyRegistry registry) {
        registry.add("valkey.query.read-write.write.nodes[0].host", () -> getenv("VALKEY_RW_WRITE_HOST", "localhost"));
        registry.add("valkey.query.read-write.write.nodes[0].port", () -> getenvInt("VALKEY_RW_WRITE_PORT", 6381));
        registry.add("valkey.query.read-write.read.nodes[0].host", () -> getenv("VALKEY_RW_READ_HOST", "localhost"));
        registry.add("valkey.query.read-write.read.nodes[0].port", () -> getenvInt("VALKEY_RW_READ_PORT", 6382));
        registry.add("valkey.query.username", () -> getenv("VALKEY_RW_USERNAME", ""));
        registry.add("valkey.query.password", () -> getenv("VALKEY_RW_PASSWORD", ""));
    }

    @Test
    void routingUsesPrimaryForWritesAndReplicaForReads() throws Exception {
        assertEquals("master", roleOf(clientRouting.executeWrite(new String[]{"ROLE"})));
        assertEquals("slave", roleOf(clientRouting.executeRead(new String[]{"ROLE"})));
        assertTrue(connectionInfo.describe().contains("mode=READ_WRITE_SPLIT"));
    }

    @Test
    void repositoryWritesToPrimaryAndReadsFromReplica() throws Exception {
        skuRepository.checkAndCreateIndex();

        String token = "rw" + UUID.randomUUID().toString().replace("-", "");
        Sku sku = new Sku(
                token,
                "ReplicaSafe" + token,
                1888,
                List.of("HOT", "RW"),
                new Merchant("Replica Merchant", "R"));

        skuRepository.save(sku);

        SkuQuery q = new SkuQuery();
        awaitReplica(token, q);

        Sku loaded = skuRepository.findById(token);
        Page<Sku> page = skuRepository.queryChain()
                .where(q.id.eq(token))
                .and(q.tags.contains("RW"))
                .orderByAsc("price")
                .page(0, 10);

        assertNotNull(loaded);
        assertEquals(token, loaded.getId());
        assertEquals(List.of("HOT", "RW"), loaded.getTags());
        assertNotNull(loaded.getMerchant());
        assertEquals("Replica Merchant", loaded.getMerchant().getName());
        assertEquals(1L, page.total());
        assertEquals(token, page.records().get(0).getId());
    }

    @Test
    void bulkWriteAndNumericUpdatesWorkThroughReadWriteSplit() throws Exception {
        skuRepository.checkAndCreateIndex();

        String token = "rwb" + UUID.randomUUID().toString().replace("-", "");
        Sku first = new Sku(token + "-1", "RW-Bulk-" + token + "-1", 1001, List.of("RWB", token), new Merchant("RW-M1", "A"));
        Sku second = new Sku(token + "-2", "RW-Bulk-" + token + "-2", 1002, List.of("RWB", token), new Merchant("RW-M2", "A"));

        BulkWriteResult saved = skuRepository.saveAll(List.of(
                BulkSaveItem.of(first.getId(), first),
                BulkSaveItem.of(second.getId(), second)
        ), BulkWriteOptions.unordered());

        assertEquals(2, saved.submitted());
        assertEquals(2, saved.succeeded());
        awaitReplicaCount(token, 2L);

        SkuQuery q = new SkuQuery();
        long incremented = skuRepository.updateChain()
                .where(q.id.eq(first.getId()))
                .increment(q.price, 10)
                .execute();
        long decremented = skuRepository.updateChain()
                .where(q.id.eq(second.getId()))
                .decrement(q.price, 2)
                .execute();

        assertEquals(1L, incremented);
        assertEquals(1L, decremented);
        awaitReplicaPrice(first.getId(), 1011);
        awaitReplicaPrice(second.getId(), 1000);

        BulkWriteResult deleted = skuRepository.deleteAll(List.of(first.getId(), second.getId()), BulkWriteOptions.unordered());
        assertEquals(2, deleted.submitted());
        assertEquals(2, deleted.succeeded());
        awaitReplicaCount(token, 0L);
    }

    @Test
    void orderedBulkWriteWorksThroughReadWriteSplit() throws Exception {
        skuRepository.checkAndCreateIndex();

        String token = "rwbo" + UUID.randomUUID().toString().replace("-", "");
        List<Sku> skus = List.of(
                new Sku(token + "-1", "RW-Ordered-" + token + "-1", 1201, List.of("RW_ORDERED", token), new Merchant("RWO1", "A")),
                new Sku(token + "-2", "RW-Ordered-" + token + "-2", 1202, List.of("RW_ORDERED", token), new Merchant("RWO2", "A"))
        );

        BulkWriteResult saved = skuRepository.saveAll(
                skus.stream().map(sku -> BulkSaveItem.of(sku.getId(), sku)).toList(),
                BulkWriteOptions.ordered()
        );
        assertEquals(2, saved.submitted());
        assertEquals(2, saved.succeeded());
        awaitReplicaCount(token, 2L);

        BulkWriteResult deleted = skuRepository.deleteAll(
                skus.stream().map(Sku::getId).toList(),
                BulkWriteOptions.ordered()
        );
        assertEquals(2, deleted.submitted());
        assertEquals(2, deleted.succeeded());
        awaitReplicaCount(token, 0L);
    }

    @Test
    void bulkUpdateWorksThroughReadWriteSplit() throws Exception {
        skuRepository.checkAndCreateIndex();

        String token = "rwup" + UUID.randomUUID().toString().replace("-", "");
        Sku first = new Sku(token + "-1", "RW-Update-" + token + "-1", 1251, List.of("RW_UPDATE", token), new Merchant("RWU1", "A"));
        Sku second = new Sku(token + "-2", "RW-Update-" + token + "-2", 1252, List.of("RW_UPDATE", token), new Merchant("RWU2", "A"));

        skuRepository.saveAll(List.of(
                BulkSaveItem.of(first.getId(), first),
                BulkSaveItem.of(second.getId(), second)
        ), BulkWriteOptions.unordered());
        awaitReplicaCount(token, 2L);

        SkuQuery q = new SkuQuery();
        BulkWriteResult updated = skuRepository.updateAll(List.of(
                BulkUpdateItem.of(
                        first.getId(),
                        new UpdateAssignment("title", "RW-Update-Updated-" + token + "-1"),
                        new NumericUpdateOperation("price", 7, UpdateOperationKind.INCREMENT)
                ).when(q.price.eq(1251)),
                BulkUpdateItem.of(
                        second.getId(),
                        new UpdateAssignment("title", "RW-Update-Updated-" + token + "-2")
                ).when(q.price.eq(9999))
        ), BulkWriteOptions.unordered());

        assertEquals(2, updated.submitted());
        assertEquals(1, updated.succeeded());
        assertEquals(1, updated.failed());
        awaitReplicaTitle(first.getId(), "RW-Update-Updated-" + token + "-1");
        awaitReplicaPrice(first.getId(), 1258);

        Sku secondLoaded = skuRepository.findById(second.getId());
        assertEquals("RW-Update-" + token + "-2", secondLoaded.getTitle());
        assertEquals(1252, secondLoaded.getPrice());
    }

    @Test
    void optimisticVersionUpdateWorksThroughReadWriteSplit() throws Exception {
        skuRepository.checkAndCreateIndex();

        String token = "rwv" + UUID.randomUUID().toString().replace("-", "");
        Sku sku = new Sku(
                token,
                "RW-Version-" + token,
                1300,
                List.of("RWV", token),
                new Merchant("RW-Version", "A"));
        skuRepository.save(sku);

        String key = skuRepository.getPrefix() + token;
        clientRouting.executeWrite(new String[]{"JSON.SET", key, "$.version", "1"});
        awaitReplicaVersion(token, 1L);

        ValkeyFieldReference versionField = () -> "version";
        long updated = skuRepository.updateChain()
                .whereId(token)
                .expectVersion(versionField, 1)
                .advanceVersion(versionField, 1)
                .set("title", "RW-Version-Updated-" + token)
                .execute();

        assertEquals(1L, updated);
        awaitReplicaVersion(token, 2L);
        awaitReplicaTitle(token, "RW-Version-Updated-" + token);
    }

    @Test
    void facetChainWorksThroughReadWriteSplit() throws Exception {
        skuRepository.checkAndCreateIndex();

        String token = "rwf" + UUID.randomUUID().toString().replace("-", "");
        Sku first = new Sku(token + "-1", "RW-Facet-" + token + "-1", 1401, List.of("RWFACET", token, "HOT"), new Merchant("RWF1", "A"));
        Sku second = new Sku(token + "-2", "RW-Facet-" + token + "-2", 1402, List.of("RWFACET", token, "HOT"), new Merchant("RWF2", "A"));
        Sku third = new Sku(token + "-3", "RW-Facet-" + token + "-3", 1403, List.of("RWFACET", token, "NEW"), new Merchant("RWF3", "A"));

        skuRepository.saveAll(List.of(
                BulkSaveItem.of(first.getId(), first),
                BulkSaveItem.of(second.getId(), second),
                BulkSaveItem.of(third.getId(), third)
        ), BulkWriteOptions.unordered());

        awaitReplicaCount(token, 3L);

        SkuQuery q = new SkuQuery();
        FacetResult facet = skuRepository.facetChain()
                .where(q.tags.contains(token))
                .on(q.tags)
                .size(10)
                .sortByCountDesc()
                .result();

        assertEquals("tags", facet.fieldName());
        assertTrue(facet.total() >= 1L);
        assertFalse(facet.buckets().isEmpty());
        long covered = facet.buckets().stream().mapToLong(com.momao.valkey.core.FacetBucket::count).sum();
        assertTrue(covered >= 3L);

        FacetResult filteredFacet = skuRepository.facetChain()
                .where(q.tags.contains(token))
                .on(q.tags)
                .minCount(2)
                .size(10)
                .sortByCountDesc()
                .result();

        assertEquals("tags", filteredFacet.fieldName());
        assertEquals(1L, filteredFacet.total());
        assertEquals(1, filteredFacet.buckets().size());
        assertTrue(filteredFacet.buckets().get(0).value().contains("HOT"));
        assertEquals(2L, filteredFacet.buckets().get(0).count());
    }

    @Test
    void facetIncludeValuesWorksThroughReadWriteSplit() throws Exception {
        skuRepository.checkAndCreateIndex();

        String token = "rwfinc" + UUID.randomUUID().toString().replace("-", "");
        String merchantA = "RWFI-" + token + "-A";
        String merchantB = "RWFI-" + token + "-B";
        skuRepository.saveAll(List.of(
                BulkSaveItem.of(token + "-1", new Sku(token + "-1", "RW-FacetInc-" + token + "-1", 1411, List.of("RW_FACET_INCLUDE", token), new Merchant(merchantA, "A"))),
                BulkSaveItem.of(token + "-2", new Sku(token + "-2", "RW-FacetInc-" + token + "-2", 1412, List.of("RW_FACET_INCLUDE", token), new Merchant(merchantA, "A"))),
                BulkSaveItem.of(token + "-3", new Sku(token + "-3", "RW-FacetInc-" + token + "-3", 1413, List.of("RW_FACET_INCLUDE", token), new Merchant(merchantB, "A")))
        ), BulkWriteOptions.unordered());

        awaitReplicaCount(token, 3L);

        SkuQuery q = new SkuQuery();
        FacetResult facet = skuRepository.facetChain()
                .where(q.tags.contains(token))
                .on(q.merchant.name)
                .includeValues(merchantB)
                .size(10)
                .sortByCountDesc()
                .result();

        assertEquals("merchant_name", facet.fieldName());
        assertEquals(1L, facet.total());
        assertEquals(1, facet.buckets().size());
        assertEquals(merchantB, facet.buckets().get(0).value());
        assertEquals(1L, facet.buckets().get(0).count());
    }

    @Test
    void projectionWorksThroughReadWriteSplit() throws Exception {
        skuRepository.checkAndCreateIndex();

        String token = "rwp" + UUID.randomUUID().toString().replace("-", "");
        Sku sku = new Sku(token, "RW-Projection-" + token, 1501, List.of("RW_PROJECTION", token), new Merchant("RW-P", "A"));
        skuRepository.save(sku);
        awaitReplica(token, new SkuQuery());

        Sku projected = skuRepository.queryChain()
                .where(new SkuQuery().id.eq(token))
                .select("title", "merchant_name")
                .one();

        assertNotNull(projected);
        assertEquals(token, projected.getId());
        assertEquals("RW-Projection-" + token, projected.getTitle());
        assertNotNull(projected.getMerchant());
        assertEquals("RW-P", projected.getMerchant().getName());
        assertEquals(null, projected.getPrice());
    }

    @Test
    void countDistinctWorksThroughReadWriteSplit() throws Exception {
        skuRepository.checkAndCreateIndex();

        String token = "rwad" + UUID.randomUUID().toString().replace("-", "");
        skuRepository.saveAll(List.of(
                BulkSaveItem.of(token + "-1", new Sku(token + "-1", "RW-Agg-" + token + "-1", 1601, List.of("RW_AGG", token, "HOT"), new Merchant("RW-M1", "A"))),
                BulkSaveItem.of(token + "-2", new Sku(token + "-2", "RW-Agg-" + token + "-2", 1602, List.of("RW_AGG", token, "HOT"), new Merchant("RW-M2", "A"))),
                BulkSaveItem.of(token + "-3", new Sku(token + "-3", "RW-Agg-" + token + "-3", 1603, List.of("RW_AGG", token, "NEW"), new Merchant("RW-M2", "A")))
        ), BulkWriteOptions.unordered());

        awaitReplicaCount(token, 3L);

        SkuQuery q = new SkuQuery();
        AggregateResult result = skuRepository.aggregateChain()
                .where(q.tags.contains(token))
                .groupBy(q.tags)
                .countDistinct(q.merchant.name, "merchant_count")
                .count("count")
                .sortByDesc("count")
                .result();

        assertEquals(2L, result.total());
        assertEquals(2L, result.rows().get(0).getLong("merchant_count"));
        assertEquals(2L, result.rows().get(0).getLong("count"));
        assertEquals(1L, result.rows().get(1).getLong("merchant_count"));
        assertEquals(1L, result.rows().get(1).getLong("count"));
    }

    @Test
    void typedProjectionMapperWorksThroughReadWriteSplit() throws Exception {
        skuRepository.checkAndCreateIndex();

        String token = "rwpm" + UUID.randomUUID().toString().replace("-", "");
        Sku sku = new Sku(token, "RW-ProjectionMap-" + token, 1511, List.of("RW_PROJECTION_MAP", token), new Merchant("RW-PM", "A"));
        skuRepository.save(sku);
        awaitReplica(token, new SkuQuery());

        SkuProjectionView projected = skuRepository.queryChain()
                .where(new SkuQuery().id.eq(token))
                .select("title", "merchant_name")
                .one(value -> new SkuProjectionView(
                        value.getId(),
                        value.getTitle(),
                        value.getMerchant() == null ? null : value.getMerchant().getName()
                ));

        assertNotNull(projected);
        assertEquals(token, projected.id());
        assertEquals("RW-ProjectionMap-" + token, projected.title());
        assertEquals("RW-PM", projected.merchantName());
    }

    @Test
    void distinctWorksThroughReadWriteSplit() throws Exception {
        skuRepository.checkAndCreateIndex();

        String token = "rwdistinct" + UUID.randomUUID().toString().replace("-", "");
        skuRepository.saveAll(List.of(
                BulkSaveItem.of(token + "-1", new Sku(token + "-1", "RW-Distinct-" + token + "-1", 1701, List.of("RW_DISTINCT", token), new Merchant("RW-D1", "A"))),
                BulkSaveItem.of(token + "-2", new Sku(token + "-2", "RW-Distinct-" + token + "-2", 1702, List.of("RW_DISTINCT", token), new Merchant("RW-D1", "A"))),
                BulkSaveItem.of(token + "-3", new Sku(token + "-3", "RW-Distinct-" + token + "-3", 1703, List.of("RW_DISTINCT", token), new Merchant("RW-D2", "A")))
        ), BulkWriteOptions.unordered());
        awaitReplicaCount(token, 3L);

        SkuQuery q = new SkuQuery();
        List<String> merchants = skuRepository.queryChain()
                .where(q.tags.contains(token))
                .distinct(q.merchant.name);

        assertEquals(2, merchants.size());
        assertTrue(merchants.contains("RW-D1"));
        assertTrue(merchants.contains("RW-D2"));
    }

    @Test
    void countDistinctShortcutWorksThroughReadWriteSplit() throws Exception {
        skuRepository.checkAndCreateIndex();

        String token = "rwcountdistinct" + UUID.randomUUID().toString().replace("-", "");
        skuRepository.saveAll(List.of(
                BulkSaveItem.of(token + "-1", new Sku(token + "-1", "RW-CD-" + token + "-1", 1704, List.of("RW_COUNT_DISTINCT", token, "HOT"), new Merchant("RW-CD-M1", "A"))),
                BulkSaveItem.of(token + "-2", new Sku(token + "-2", "RW-CD-" + token + "-2", 1705, List.of("RW_COUNT_DISTINCT", token, "HOT"), new Merchant("RW-CD-M1", "A"))),
                BulkSaveItem.of(token + "-3", new Sku(token + "-3", "RW-CD-" + token + "-3", 1706, List.of("RW_COUNT_DISTINCT", token, "NEW"), new Merchant("RW-CD-M2", "A")))
        ), BulkWriteOptions.unordered());
        awaitReplicaCount(token, 3L);

        SkuQuery q = new SkuQuery();
        long merchantCount = skuRepository.queryChain()
                .where(q.tags.contains(token))
                .countDistinct(q.merchant.name);

        assertEquals(2L, merchantCount);
    }

    @Test
    void distinctRowsWorkThroughReadWriteSplit() throws Exception {
        skuRepository.checkAndCreateIndex();

        String token = "rwdr" + UUID.randomUUID().toString().replace("-", "");
        skuRepository.saveAll(List.of(
                BulkSaveItem.of(token + "-1", new Sku(token + "-1", "RW-DR-" + token + "-1", 1711, List.of("RW_DR", token, "HOT"), new Merchant("RW-DR-M1", "A"))),
                BulkSaveItem.of(token + "-2", new Sku(token + "-2", "RW-DR-" + token + "-2", 1712, List.of("RW_DR", token, "HOT"), new Merchant("RW-DR-M1", "A"))),
                BulkSaveItem.of(token + "-3", new Sku(token + "-3", "RW-DR-" + token + "-3", 1713, List.of("RW_DR", token, "NEW"), new Merchant("RW-DR-M2", "A")))
        ), BulkWriteOptions.unordered());
        awaitReplicaCount(token, 3L);

        SkuQuery q = new SkuQuery();
        List<String> pairs = skuRepository.queryChain()
                .where(q.tags.contains(token))
                .distinctRows(row -> row.getString("tags") + ":" + row.getString("merchant_name"),
                        q.tags,
                        q.merchant.name);

        assertEquals(2, pairs.size());
        assertTrue(pairs.stream().anyMatch(pair -> pair.contains("HOT:RW-DR-M1")));
        assertTrue(pairs.stream().anyMatch(pair -> pair.contains("NEW:RW-DR-M2")));
    }

    @Test
    void distinctRowsTypeMappingWorksThroughReadWriteSplit() throws Exception {
        skuRepository.checkAndCreateIndex();

        String token = "rwdrtype" + UUID.randomUUID().toString().replace("-", "");
        skuRepository.saveAll(List.of(
                BulkSaveItem.of(token + "-1", new Sku(token + "-1", "RW-DRT-" + token + "-1", 1714, List.of("RW_DRT", token, "HOT"), new Merchant("RW-DRT-M1", "A"))),
                BulkSaveItem.of(token + "-2", new Sku(token + "-2", "RW-DRT-" + token + "-2", 1715, List.of("RW_DRT", token, "NEW"), new Merchant("RW-DRT-M2", "A")))
        ), BulkWriteOptions.unordered());
        awaitReplicaCount(token, 2L);

        SkuQuery q = new SkuQuery();
        List<TagMerchantPair> rows = skuRepository.queryChain()
                .where(q.tags.contains(token))
                .distinctRows(TagMerchantPair.class, q.tags, q.merchant.name);

        assertEquals(2, rows.size());
        assertTrue(rows.stream().anyMatch(row -> row.tags() != null && row.tags().contains("HOT") && "RW-DRT-M1".equals(row.merchant_name())));
        assertTrue(rows.stream().anyMatch(row -> row.tags() != null && row.tags().contains("NEW") && "RW-DRT-M2".equals(row.merchant_name())));
    }

    @Test
    void aggregateAndMapRowsWorkThroughReadWriteSplit() throws Exception {
        HashStudentReadWriteRepository repository = new HashStudentReadWriteRepository(clientRouting, objectMapper);
        repository.checkAndCreateIndex();

        String token = "rwa" + UUID.randomUUID().toString().replace("-", "");
        String departmentA = "Dept-" + token + "-A";
        String departmentB = "Dept-" + token + "-B";

        repository.saveStudent(token + "-1", "Alice-" + token, 18, 90.0d, "Class-" + token + "-1", departmentA, "ON");
        repository.saveStudent(token + "-2", "Bob-" + token, 19, 80.0d, "Class-" + token + "-1", departmentA, "ON");
        repository.saveStudent(token + "-3", "Carol-" + token, 20, 70.0d, "Class-" + token + "-2", departmentB, "ON");
        awaitStudentReplicaIndexed(repository, departmentA, departmentB, 3L);

        StudentQuery q = new StudentQuery();
        AggregateResult result = repository.aggregateChain()
                .where(q.department.eq(departmentA).or(q.department.eq(departmentB)))
                .and(q.status.eq("ON"))
                .groupBy(q.department)
                .count("count")
                .sum(q.score, "score_sum")
                .avg(q.score, "avg_score")
                .min(q.age, "age_min")
                .max(q.age, "age_max")
                .sortByDesc("count")
                .limit(0, 10)
                .result();

        List<DepartmentAggregateRow> rows = result.mapRows(DepartmentAggregateRow.class);
        assertEquals(2L, result.total());
        assertEquals(2, rows.size());
        DepartmentAggregateRow first = rows.stream()
                .filter(row -> departmentA.equals(row.department()))
                .findFirst()
                .orElseThrow();
        DepartmentAggregateRow second = rows.stream()
                .filter(row -> departmentB.equals(row.department()))
                .findFirst()
                .orElseThrow();
        assertEquals(2L, first.count());
        assertEquals(170.0d, first.score_sum());
        assertEquals(85.0d, first.avg_score());
        assertEquals(18L, first.age_min());
        assertEquals(19L, first.age_max());
        assertEquals(1L, second.count());
        assertEquals(70.0d, second.score_sum());
        assertEquals(70.0d, second.avg_score());
        assertEquals(20L, second.age_min());
        assertEquals(20L, second.age_max());
    }

    @Test
    void aggregateApplyAndFilterWorkThroughReadWriteSplit() throws Exception {
        HashStudentReadWriteRepository repository = new HashStudentReadWriteRepository(clientRouting, objectMapper);
        repository.checkAndCreateIndex();

        String token = "rwe" + UUID.randomUUID().toString().replace("-", "");
        String departmentA = "Dept-" + token + "-A";
        String departmentB = "Dept-" + token + "-B";

        repository.saveStudent(token + "-1", "Alice-" + token, 18, 90.0d, "Class-" + token + "-1", departmentA, "ON");
        repository.saveStudent(token + "-2", "Bob-" + token, 19, 80.0d, "Class-" + token + "-1", departmentA, "ON");
        repository.saveStudent(token + "-3", "Carol-" + token, 20, 70.0d, "Class-" + token + "-2", departmentB, "ON");
        awaitStudentReplicaIndexed(repository, departmentA, departmentB, 3L);

        StudentQuery q = new StudentQuery();
        AggregateResult result = repository.aggregateChain()
                .where(q.department.eq(departmentA).or(q.department.eq(departmentB)))
                .and(q.status.eq("ON"))
                .groupBy(q.department)
                .count("count")
                .apply(AggregateExpressions.field("count").multiply(10), "weighted_count")
                .filter(AggregateExpressions.field("weighted_count").gt(10))
                .sortByDesc("weighted_count")
                .result();

        assertEquals(1L, result.total());
        assertEquals(departmentA, result.rows().get(0).getString("department"));
        assertEquals(20L, result.rows().get(0).getLong("weighted_count"));
    }

    private void awaitReplica(String token, SkuQuery q) throws Exception {
        long deadline = System.currentTimeMillis() + 8_000L;
        while (System.currentTimeMillis() < deadline) {
            if (isReplicaVisible(token, q)) {
                return;
            }
            Thread.sleep(200L);
        }
        fail("主从复制超时，读节点未在规定时间内可见文档: " + token);
    }

    private void awaitReplicaCount(String token, long expected) throws Exception {
        SkuQuery q = new SkuQuery();
        long deadline = System.currentTimeMillis() + 8_000L;
        while (System.currentTimeMillis() < deadline) {
            long total = skuRepository.queryChain()
                    .where(q.tags.contains(token))
                    .count();
            if (total == expected) {
                return;
            }
            Thread.sleep(200L);
        }
        fail("主从复制超时，读节点统计未在规定时间内达到预期: token=" + token + ", expected=" + expected);
    }

    private void awaitReplicaPrice(String id, int expectedPrice) throws Exception {
        long deadline = System.currentTimeMillis() + 8_000L;
        while (System.currentTimeMillis() < deadline) {
            Sku loaded = skuRepository.findById(id);
            if (loaded != null && loaded.getPrice() != null && loaded.getPrice() == expectedPrice) {
                return;
            }
            Thread.sleep(200L);
        }
        Sku loaded = skuRepository.findById(id);
        fail("主从复制超时，读节点价格未在规定时间内达到预期: id=" + id + ", expected=" + expectedPrice + ", actual=" + (loaded == null ? "null" : loaded.getPrice()));
    }

    private void awaitReplicaVersion(String id, long expectedVersion) throws Exception {
        String key = skuRepository.getPrefix() + id;
        long deadline = System.currentTimeMillis() + 8_000L;
        while (System.currentTimeMillis() < deadline) {
            Object rawVersion = clientRouting.executeRead(new String[]{"JSON.GET", key, "$.version"});
            if (rawVersion != null && !"null".equalsIgnoreCase(String.valueOf(rawVersion))) {
                String text = String.valueOf(rawVersion).trim();
                if (text.startsWith("[") && text.endsWith("]")) {
                    String inner = text.substring(1, text.length() - 1).replace("\"", "").trim();
                    if (!inner.isEmpty() && Long.parseLong(inner) == expectedVersion) {
                        return;
                    }
                }
            }
            Thread.sleep(200L);
        }
        Object rawVersion = clientRouting.executeRead(new String[]{"JSON.GET", key, "$.version"});
        fail("主从复制超时，读节点版本未在规定时间内达到预期: id=" + id + ", expected=" + expectedVersion + ", actual=" + rawVersion);
    }

    private void awaitReplicaTitle(String id, String expectedTitle) throws Exception {
        String key = skuRepository.getPrefix() + id;
        long deadline = System.currentTimeMillis() + 8_000L;
        while (System.currentTimeMillis() < deadline) {
            Object rawTitle = clientRouting.executeRead(new String[]{"JSON.GET", key, "$.title"});
            if (rawTitle != null && String.valueOf(rawTitle).contains(expectedTitle)) {
                return;
            }
            Thread.sleep(200L);
        }
        Object rawTitle = clientRouting.executeRead(new String[]{"JSON.GET", key, "$.title"});
        fail("主从复制超时，读节点标题未在规定时间内达到预期: id=" + id + ", expected=" + expectedTitle + ", actual=" + rawTitle);
    }

    private boolean isReplicaVisible(String token, SkuQuery q) throws Exception {
        Object rawJson = clientRouting.executeRead(new String[]{"JSON.GET", skuRepository.getPrefix() + token});
        if (rawJson != null && !"null".equalsIgnoreCase(String.valueOf(rawJson))) {
            long total = skuRepository.queryChain()
                    .where(q.id.eq(token))
                    .count();
            return total > 0;
        }
        return false;
    }

    private String roleOf(Object response) {
        if (response instanceof Object[] array && array.length > 0) {
            return String.valueOf(array[0]).toLowerCase(Locale.ROOT);
        }
        return String.valueOf(response).toLowerCase(Locale.ROOT);
    }

    private void awaitStudentReplicaIndexed(HashStudentReadWriteRepository repository, String departmentA, String departmentB, long expected) throws Exception {
        StudentQuery q = new StudentQuery();
        long deadline = System.currentTimeMillis() + 8_000L;
        while (System.currentTimeMillis() < deadline) {
            long total = repository.queryChain()
                    .where(q.department.eq(departmentA).or(q.department.eq(departmentB)))
                    .count();
            if (total >= expected) {
                return;
            }
            Thread.sleep(200L);
        }
        fail("主从复制超时，学生聚合测试文档未在规定时间内进入索引");
    }

    record DepartmentAggregateRow(String department, long count, double score_sum, double avg_score, long age_min, long age_max) {
    }

    record TagMerchantPair(String tags, String merchant_name) {
    }

    private static final class HashStudentReadWriteRepository extends BaseValkeyRepository<Student> {

        private HashStudentReadWriteRepository(ValkeyClientRouting routing, ObjectMapper objectMapper) {
            super(StudentQuery.METADATA, routing, Student.class, objectMapper);
        }

        private void saveStudent(String id, String name, int age, double score, String className, String department, String status) {
            Student student = new Student();
            student.setId(Math.abs((long) id.hashCode()));
            student.setName(name);
            student.setAge(age);
            student.setScore(score);
            student.setClassName(className);
            student.setDepartment(department);
            student.setStatus(status);
            save(id, student);
        }
    }

    private static String getenv(String key, String fallback) {
        String value = System.getenv(key);
        return value == null || value.isBlank() ? fallback : value;
    }

    private static Integer getenvInt(String key, int fallback) {
        String value = System.getenv(key);
        if (value == null || value.isBlank()) {
            return fallback;
        }
        return Integer.parseInt(value);
    }
}
