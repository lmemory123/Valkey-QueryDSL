package com.momao.valkey.example;

import com.momao.valkey.adapter.ValkeyClientRouting;
import com.momao.valkey.core.BulkSaveItem;
import com.momao.valkey.core.BulkUpdateItem;
import com.momao.valkey.core.BulkWriteOptions;
import com.momao.valkey.core.BulkWriteResult;
import com.momao.valkey.core.NumericUpdateOperation;
import com.momao.valkey.core.UpdateAssignment;
import com.momao.valkey.core.UpdateOperationKind;
import com.momao.valkey.core.ValkeyFieldReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(classes = ValkeyQueryDemoApplication.class, webEnvironment = SpringBootTest.WebEnvironment.NONE)
@EnabledIfEnvironmentVariable(named = "VALKEY_CLUSTER_INTEGRATION", matches = "true")
@TestPropertySource(properties = {
        "valkey.query.enabled=true",
        "valkey.query.mode=cluster"
})
class ClusterBulkWriteIntegrationTests {

    record SkuProjectionView(String id, String title, String merchantName) {
    }

    @Autowired
    private ValkeyClientRouting clientRouting;

    @Autowired
    private SkuRepository skuRepository;

    @DynamicPropertySource
    static void registerProperties(DynamicPropertyRegistry registry) {
        registry.add("valkey.query.cluster.nodes[0].host", () -> getenv("VALKEY_CLUSTER_HOST", "localhost"));
        registry.add("valkey.query.cluster.nodes[0].port", () -> getenvInt("VALKEY_CLUSTER_PORT", 8000));
    }

    @BeforeEach
    void ensureSkuIndexExists() {
        skuRepository.checkAndCreateIndex();
    }

    @Test
    void saveAllAndDeleteAllWorkAgainstClusterValkey() throws Exception {
        String token = "cbulk" + UUID.randomUUID().toString().replace("-", "");
        List<Sku> skus = List.of(
                new Sku(token + "-1", "ClusterBulk-" + token + "-1", 2001, List.of("CLUSTER_BULK", token), new Merchant("C1", "A")),
                new Sku(token + "-2", "ClusterBulk-" + token + "-2", 2002, List.of("CLUSTER_BULK", token), new Merchant("C2", "A")),
                new Sku(token + "-3", "ClusterBulk-" + token + "-3", 2003, List.of("CLUSTER_BULK", token), new Merchant("C3", "A"))
        );

        BulkWriteResult saved = skuRepository.saveAll(
                skus.stream().map(sku -> BulkSaveItem.of(sku.getId(), sku)).toList(),
                BulkWriteOptions.unordered()
        );
        assertEquals(3, saved.submitted());
        assertEquals(3, saved.succeeded());
        awaitIndexedCount(token, 3L);

        BulkWriteResult deleted = skuRepository.deleteAll(
                skus.stream().map(Sku::getId).toList(),
                BulkWriteOptions.unordered()
        );
        assertEquals(3, deleted.submitted());
        assertEquals(3, deleted.succeeded());
        awaitIndexedCount(token, 0L);
    }

    @Test
    void orderedSaveAllAndDeleteAllWorkAgainstClusterValkey() throws Exception {
        String token = "cbulko" + UUID.randomUUID().toString().replace("-", "");
        List<Sku> skus = List.of(
                new Sku(token + "-1", "ClusterBulkOrd-" + token + "-1", 2101, List.of("CLUSTER_BULK_ORDERED", token), new Merchant("CO1", "A")),
                new Sku(token + "-2", "ClusterBulkOrd-" + token + "-2", 2102, List.of("CLUSTER_BULK_ORDERED", token), new Merchant("CO2", "A"))
        );

        BulkWriteResult saved = skuRepository.saveAll(
                skus.stream().map(sku -> BulkSaveItem.of(sku.getId(), sku)).toList(),
                BulkWriteOptions.ordered()
        );
        assertEquals(2, saved.submitted());
        assertEquals(2, saved.succeeded());
        awaitIndexedCount(token, 2L);

        BulkWriteResult deleted = skuRepository.deleteAll(
                skus.stream().map(Sku::getId).toList(),
                BulkWriteOptions.ordered()
        );
        assertEquals(2, deleted.submitted());
        assertEquals(2, deleted.succeeded());
        awaitIndexedCount(token, 0L);
    }

    @Test
    void numericUpdatesWorkAgainstClusterValkey() throws Exception {
        String token = "cnum" + UUID.randomUUID().toString().replace("-", "");
        Sku sku = new Sku(token, "ClusterNumeric-" + token, 500, List.of("CLUSTER_NUMERIC", token), new Merchant("CN", "A"));
        skuRepository.save(sku);
        awaitIndexedCount(token, 1L);

        SkuQuery q = new SkuQuery();
        long incremented = skuRepository.updateChain()
                .where(q.id.eq(token))
                .increment(q.price, 25)
                .execute();
        long decremented = skuRepository.updateChain()
                .where(q.id.eq(token))
                .decrement(q.price, 5)
                .execute();

        assertEquals(1L, incremented);
        assertEquals(1L, decremented);

        long deadline = System.currentTimeMillis() + 8_000L;
        while (System.currentTimeMillis() < deadline) {
            Sku loaded = skuRepository.findById(token);
            if (loaded != null && loaded.getPrice() != null && loaded.getPrice() == 520) {
                return;
            }
            Thread.sleep(100L);
        }
        Sku loaded = skuRepository.findById(token);
        assertEquals(520, loaded == null || loaded.getPrice() == null ? -1 : loaded.getPrice());
    }

    @Test
    void projectionWorksAgainstClusterValkey() throws Exception {
        String token = "cproj" + UUID.randomUUID().toString().replace("-", "");
        Sku sku = new Sku(token, "ClusterProjection-" + token, 901, List.of("CLUSTER_PROJECTION", token), new Merchant("CP", "A"));
        skuRepository.save(sku);
        awaitIndexedCount(token, 1L);

        Sku projected = skuRepository.queryChain()
                .where(new SkuQuery().id.eq(token))
                .select("title", "merchant_name")
                .one();

        assertEquals(token, projected.getId());
        assertEquals("ClusterProjection-" + token, projected.getTitle());
        assertEquals("CP", projected.getMerchant().getName());
        assertEquals(null, projected.getPrice());
    }

    @Test
    void projectionWithNestedAndCollectionFieldsWorksAgainstClusterValkey() throws Exception {
        String token = "cprojx" + UUID.randomUUID().toString().replace("-", "");
        Sku sku = new Sku(token, "ClusterProjectionMix-" + token, 902, List.of("CLUSTER_PROJECTION_MIX", token, "HOT"), new Merchant("CPX", "VIP"));
        skuRepository.save(sku);
        awaitIndexedCount(token, 1L);

        Sku projected = skuRepository.queryChain()
                .where(new SkuQuery().id.eq(token))
                .select("title", "merchant_name", "tags")
                .one();

        assertEquals(token, projected.getId());
        assertEquals("ClusterProjectionMix-" + token, projected.getTitle());
        assertEquals(List.of("CLUSTER_PROJECTION_MIX", token, "HOT"), projected.getTags());
        assertEquals("CPX", projected.getMerchant().getName());
        assertEquals(null, projected.getMerchant().getLevel());
        assertEquals(null, projected.getPrice());
    }

    @Test
    void typedProjectionMapperWorksAgainstClusterValkey() throws Exception {
        String token = "cprojm" + UUID.randomUUID().toString().replace("-", "");
        Sku sku = new Sku(token, "ClusterProjectionMap-" + token, 903, List.of("CLUSTER_PROJECTION_MAP", token), new Merchant("CPM", "A"));
        skuRepository.save(sku);
        awaitIndexedCount(token, 1L);

        SkuProjectionView projected = skuRepository.queryChain()
                .where(new SkuQuery().id.eq(token))
                .select("title", "merchant_name")
                .one(value -> new SkuProjectionView(
                        value.getId(),
                        value.getTitle(),
                        value.getMerchant() == null ? null : value.getMerchant().getName()
                ));

        assertEquals(token, projected.id());
        assertEquals("ClusterProjectionMap-" + token, projected.title());
        assertEquals("CPM", projected.merchantName());
    }

    @Test
    void optimisticVersionUpdateWorksAgainstClusterValkey() throws Exception {
        String token = "cver" + UUID.randomUUID().toString().replace("-", "");
        Sku sku = new Sku(token, "ClusterVersion-" + token, 777, List.of("CLUSTER_VERSION", token), new Merchant("CV", "A"));
        skuRepository.save(sku);
        awaitIndexedCount(token, 1L);

        String key = skuRepository.getPrefix() + token;
        clientRouting.executeWrite(new String[]{"JSON.SET", key, "$.version", "1"});
        awaitVersion(key, 1L);

        ValkeyFieldReference versionField = () -> "version";
        long updated = skuRepository.updateChain()
                .whereId(token)
                .expectVersion(versionField, 1)
                .advanceVersion(versionField, 1)
                .set("title", "ClusterVersion-Updated-" + token)
                .execute();

        assertEquals(1L, updated);
        awaitVersion(key, 2L);
        awaitTitle(key, "ClusterVersion-Updated-" + token);
    }

    @Test
    void bulkUpdateWorksAgainstClusterValkey() throws Exception {
        String token = "cupd" + UUID.randomUUID().toString().replace("-", "");
        Sku first = new Sku(token + "-1", "ClusterUpdate-" + token + "-1", 2301, List.of("CLUSTER_UPDATE", token), new Merchant("CU1", "A"));
        Sku second = new Sku(token + "-2", "ClusterUpdate-" + token + "-2", 2302, List.of("CLUSTER_UPDATE", token), new Merchant("CU2", "A"));

        skuRepository.saveAll(List.of(
                BulkSaveItem.of(first.getId(), first),
                BulkSaveItem.of(second.getId(), second)
        ), BulkWriteOptions.unordered());
        awaitIndexedCount(token, 2L);

        SkuQuery q = new SkuQuery();
        BulkWriteResult updated = skuRepository.updateAll(List.of(
                BulkUpdateItem.of(
                        first.getId(),
                        new UpdateAssignment("title", "ClusterUpdate-Updated-" + token + "-1"),
                        new NumericUpdateOperation("price", 9, UpdateOperationKind.INCREMENT)
                ).when(q.price.eq(2301)),
                BulkUpdateItem.of(
                        second.getId(),
                        new UpdateAssignment("title", "ClusterUpdate-Updated-" + token + "-2")
                ).when(q.price.eq(9999))
        ), BulkWriteOptions.unordered());

        assertEquals(2, updated.submitted());
        assertEquals(1, updated.succeeded());
        assertEquals(1, updated.failed());

        long deadline = System.currentTimeMillis() + 8_000L;
        while (System.currentTimeMillis() < deadline) {
            Sku loaded = skuRepository.findById(first.getId());
            if (loaded != null && "ClusterUpdate-Updated-".concat(token).concat("-1").equals(loaded.getTitle()) && loaded.getPrice() != null && loaded.getPrice() == 2310) {
                break;
            }
            Thread.sleep(100L);
        }

        Sku firstLoaded = skuRepository.findById(first.getId());
        Sku secondLoaded = skuRepository.findById(second.getId());
        assertEquals("ClusterUpdate-Updated-" + token + "-1", firstLoaded.getTitle());
        assertEquals(2310, firstLoaded.getPrice());
        assertEquals("ClusterUpdate-" + token + "-2", secondLoaded.getTitle());
        assertEquals(2302, secondLoaded.getPrice());
    }

    private void awaitIndexedCount(String token, long expected) throws Exception {
        SkuQuery q = new SkuQuery();
        long deadline = System.currentTimeMillis() + 8_000L;
        while (System.currentTimeMillis() < deadline) {
            long total = skuRepository.queryChain()
                    .where(q.tags.contains(token))
                    .count();
            if (total == expected) {
                return;
            }
            Thread.sleep(100L);
        }
        long actual = skuRepository.queryChain().where(q.tags.contains(token)).count();
        assertTrue(actual == expected, "expected=" + expected + ", actual=" + actual);
    }

    private void awaitVersion(String key, long expectedVersion) throws Exception {
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
            Thread.sleep(100L);
        }
        assertEquals(expectedVersion, Long.MIN_VALUE, "cluster version 未达到预期");
    }

    private void awaitTitle(String key, String expectedTitle) throws Exception {
        long deadline = System.currentTimeMillis() + 8_000L;
        while (System.currentTimeMillis() < deadline) {
            Object rawTitle = clientRouting.executeRead(new String[]{"JSON.GET", key, "$.title"});
            if (rawTitle != null && String.valueOf(rawTitle).contains(expectedTitle)) {
                return;
            }
            Thread.sleep(100L);
        }
        assertEquals(expectedTitle, null, "cluster title 未达到预期");
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
