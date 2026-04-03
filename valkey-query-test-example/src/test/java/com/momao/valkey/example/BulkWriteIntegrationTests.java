package com.momao.valkey.example;

import com.momao.valkey.core.BulkSaveItem;
import com.momao.valkey.core.BulkUpdateItem;
import com.momao.valkey.core.BulkWriteOptions;
import com.momao.valkey.core.BulkWriteResult;
import com.momao.valkey.core.NumericUpdateOperation;
import com.momao.valkey.core.UpdateAssignment;
import com.momao.valkey.core.UpdateOperationKind;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(classes = ValkeyQueryDemoApplication.class, webEnvironment = SpringBootTest.WebEnvironment.NONE)
@EnabledIfEnvironmentVariable(named = "VALKEY_INTEGRATION", matches = "true")
class BulkWriteIntegrationTests {

    @Autowired
    private SkuRepository skuRepository;

    @Test
    void saveAllAndDeleteAllWorkAgainstStandaloneValkey() throws Exception {
        String token = "bulk" + UUID.randomUUID().toString().replace("-", "");
        Sku first = new Sku(token + "-1", "Bulk-" + token + "-1", 1001, List.of("BULK", token), new Merchant("M1", "A"));
        Sku second = new Sku(token + "-2", "Bulk-" + token + "-2", 1002, List.of("BULK", token), new Merchant("M2", "A"));
        Sku third = new Sku(token + "-3", "Bulk-" + token + "-3", 1003, List.of("BULK", token), new Merchant("M3", "A"));

        BulkWriteResult saved = skuRepository.saveAll(List.of(
                BulkSaveItem.of(first.getId(), first),
                BulkSaveItem.of(second.getId(), second),
                BulkSaveItem.of(third.getId(), third)
        ), BulkWriteOptions.unordered());

        assertEquals(3, saved.submitted());
        assertEquals(3, saved.succeeded());
        awaitIndexedCount(token, 3L);

        SkuQuery q = new SkuQuery();
        long total = skuRepository.queryChain()
                .where(q.tags.contains(token))
                .count();
        assertEquals(3L, total);
        assertNotNull(skuRepository.findById(first.getId()));

        BulkWriteResult deleted = skuRepository.deleteAll(
                List.of(first.getId(), second.getId(), third.getId()),
                BulkWriteOptions.unordered()
        );

        assertEquals(3, deleted.submitted());
        assertEquals(3, deleted.succeeded());
        awaitIndexedCount(token, 0L);
        assertEquals(0L, skuRepository.queryChain().where(q.tags.contains(token)).count());
    }

    @Test
    void orderedSaveAllAndDeleteAllWorkAgainstStandaloneValkey() throws Exception {
        String token = "bulkord" + UUID.randomUUID().toString().replace("-", "");
        List<Sku> skus = List.of(
                new Sku(token + "-1", "BulkOrd-" + token + "-1", 1101, List.of("BULK_ORDERED", token), new Merchant("OM1", "A")),
                new Sku(token + "-2", "BulkOrd-" + token + "-2", 1102, List.of("BULK_ORDERED", token), new Merchant("OM2", "A"))
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
    void updateAllWorksAgainstStandaloneValkey() throws Exception {
        skuRepository.checkAndCreateIndex();

        String token = "bulkup" + UUID.randomUUID().toString().replace("-", "");
        Sku first = new Sku(token + "-1", "BulkUpdate-" + token + "-1", 1601, List.of("BULK_UPDATE", token), new Merchant("BU1", "A"));
        Sku second = new Sku(token + "-2", "BulkUpdate-" + token + "-2", 1602, List.of("BULK_UPDATE", token), new Merchant("BU2", "A"));

        skuRepository.saveAll(List.of(
                BulkSaveItem.of(first.getId(), first),
                BulkSaveItem.of(second.getId(), second)
        ), BulkWriteOptions.unordered());
        awaitIndexedCount(token, 2L);

        SkuQuery q = new SkuQuery();
        BulkWriteResult updated = skuRepository.updateAll(List.of(
                BulkUpdateItem.of(
                        first.getId(),
                        new UpdateAssignment("title", "BulkUpdate-Updated-" + token + "-1"),
                        new NumericUpdateOperation("price", 10, UpdateOperationKind.INCREMENT)
                ).when(q.price.eq(1601)),
                BulkUpdateItem.of(
                        second.getId(),
                        new UpdateAssignment("title", "BulkUpdate-Updated-" + token + "-2")
                ).when(q.price.eq(9999))
        ), BulkWriteOptions.unordered());

        assertEquals(2, updated.submitted());
        assertEquals(1, updated.succeeded());
        assertEquals(1, updated.failed());

        Sku firstLoaded = skuRepository.findById(first.getId());
        Sku secondLoaded = skuRepository.findById(second.getId());
        assertEquals("BulkUpdate-Updated-" + token + "-1", firstLoaded.getTitle());
        assertEquals(1611, firstLoaded.getPrice());
        assertEquals("BulkUpdate-" + token + "-2", secondLoaded.getTitle());
        assertEquals(1602, secondLoaded.getPrice());
    }

    private void awaitIndexedCount(String token, long expected) throws Exception {
        SkuQuery q = new SkuQuery();
        long deadline = System.currentTimeMillis() + 15_000L;
        RuntimeException lastError = null;
        Long lastObserved = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                long total = skuRepository.queryChain()
                        .where(q.tags.contains(token))
                        .count();
                lastObserved = total;
                if (total == expected) {
                    return;
                }
                lastError = null;
            } catch (RuntimeException ex) {
                // Index propagation can lag briefly in CI after bulk writes/deletes.
                lastError = ex;
            }
            Thread.sleep(100L);
        }
        String detail = lastError != null
                ? "lastError=" + lastError.getMessage()
                : "lastObserved=" + lastObserved;
        assertTrue(false, "expected=" + expected + ", " + detail);
    }
}
