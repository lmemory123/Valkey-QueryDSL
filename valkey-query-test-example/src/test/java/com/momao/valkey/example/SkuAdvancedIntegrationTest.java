package com.momao.valkey.example;

import com.momao.valkey.core.Page;
import glide.api.GlideClient;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(classes = ValkeyQueryDemoApplication.class, webEnvironment = SpringBootTest.WebEnvironment.NONE)
@EnabledIfEnvironmentVariable(named = "VALKEY_PASSWORD", matches = ".+")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SkuAdvancedIntegrationTest {

    @Autowired
    private GlideClient glideClient;

    @Autowired
    private SkuRepository skuRepository;

    private String runToken;

    @BeforeAll
    void setUp() throws Exception {
        recreateJsonIndex();
        runToken = "adv" + UUID.randomUUID().toString().replace("-", "");
        for (int price = 1; price <= 100; price++) {
            skuRepository.save(new Sku(
                    runToken + "-" + price,
                    "Advanced " + runToken + " Sku " + price,
                    price,
                    buildTags(price),
                    new Merchant(resolveMerchantName(price), resolveMerchantLevel(price))));
        }
    }

    @Test
    void pagesSortedRecords() {
        SkuQuery q = new SkuQuery();

        Page<Sku> page = skuRepository.queryChain()
            .where(q.title.contains(runToken))
            .orderByAsc("price")
            .page(20, 10);

        assertEquals(100L, page.total());
        assertEquals(10, page.records().size());
        assertEquals(21, page.records().get(0).getPrice());
        assertEquals(30, page.records().get(9).getPrice());
    }

    @Test
    void findsOneAndCountsFast() {
        SkuQuery q = new SkuQuery();
        String title = "Advanced " + runToken + " Sku 42";

        Sku one = skuRepository.queryChain()
            .where(q.title.matches("\"" + title + "\""))
            .one();
        long count = skuRepository.queryChain()
            .where(q.title.contains(runToken))
            .and(q.price.gt(50))
            .count();

        assertNotNull(one);
        assertEquals(runToken + "-42", one.getId());
        assertEquals(title, one.getTitle());
        assertEquals("Merchant-C", one.getMerchant().getName());
        assertEquals(50L, count);
    }

    @Test
    void listsByTagIntersection() {
        SkuQuery q = new SkuQuery();

        List<Sku> records = skuRepository.queryChain()
            .where(q.title.contains(runToken))
            .and(q.tags.contains("HOT"))
            .and(q.tags.contains("NEW"))
            .orderByAsc("price")
            .list();

        assertEquals(25, records.size());
        assertTrue(records.stream().allMatch(item -> item.getTags().contains("HOT")));
        assertTrue(records.stream().allMatch(item -> item.getTags().contains("NEW")));
    }

    @Test
    void listsByNestedMerchantName() {
        SkuQuery q = new SkuQuery();

        List<Sku> records = skuRepository.queryChain()
            .where(q.title.contains(runToken))
            .and(q.merchant.name.eq("Merchant-B"))
            .orderByAsc("price")
            .list();

        assertEquals(33, records.size());
        assertTrue(records.stream().allMatch(item -> "Merchant-B".equals(item.getMerchant().getName())));
    }

    @Test
    void chainsQueriesAgainstRealValkeyData() {
        SkuQuery q = new SkuQuery();

        Page<Sku> page = skuRepository.queryChain()
                .where(q.title.contains(runToken))
                .and(q.tags.contains("HOT"))
                .and(q.merchant.name.eq("Merchant-A"))
                .orderByDesc("price")
                .page(0, 5);

        assertEquals(17L, page.total());
        assertEquals(5, page.records().size());
        assertEquals(100, page.records().get(0).getPrice());
        assertEquals(76, page.records().get(4).getPrice());
        assertTrue(page.records().stream().allMatch(item -> item.getTags().contains("HOT")));
        assertTrue(page.records().stream().allMatch(item -> "Merchant-A".equals(item.getMerchant().getName())));
    }

        @Test
        void chainsListCountAndOneAgainstRealValkeyData() {
        SkuQuery q = new SkuQuery();

        List<Sku> records = skuRepository.queryChain()
            .where(q.title.contains(runToken))
            .and(q.tags.contains("HOT"))
            .and(q.merchant.name.eq("Merchant-C"))
            .orderByAsc("price")
            .list();

        long total = skuRepository.queryChain()
            .where(q.title.contains(runToken))
            .and(q.tags.contains("HOT"))
            .and(q.merchant.name.eq("Merchant-C"))
            .count();

        Sku first = skuRepository.queryChain()
            .where(q.title.contains(runToken))
            .and(q.tags.contains("HOT"))
            .and(q.merchant.name.eq("Merchant-C"))
            .orderByAsc("price")
            .one();

        assertEquals(16, records.size());
        assertEquals(16L, total);
        assertEquals(6, records.get(0).getPrice());
        assertEquals(96, records.get(records.size() - 1).getPrice());
        assertNotNull(first);
        assertEquals(6, first.getPrice());
        assertTrue(records.stream().allMatch(item -> item.getTags().contains("HOT")));
        assertTrue(records.stream().allMatch(item -> "Merchant-C".equals(item.getMerchant().getName())));
        }

    private List<String> buildTags(int price) {
        List<String> tags = new ArrayList<>();
        if (price % 2 == 0) {
            tags.add("HOT");
        }
        if (price % 4 == 0) {
            tags.add("NEW");
        }
        if (tags.isEmpty()) {
            tags.add("SALE");
        }
        return tags;
    }

    private String resolveMerchantName(int price) {
        return switch (price % 3) {
            case 0 -> "Merchant-C";
            case 1 -> "Merchant-A";
            default -> "Merchant-B";
        };
    }

    private String resolveMerchantLevel(int price) {
        return price % 2 == 0 ? "VIP" : "NORMAL";
    }

    private void recreateJsonIndex() {
        try {
            glideClient.customCommand(new String[]{"FT.DROPINDEX", skuRepository.getIndexName(), "DD"}).get();
        } catch (Exception ignored) {
        }
        skuRepository.checkAndCreateIndex();
    }
}
