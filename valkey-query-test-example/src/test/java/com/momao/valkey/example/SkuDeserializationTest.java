package com.momao.valkey.example;

import com.momao.valkey.core.Page;
import com.momao.valkey.core.SearchCondition;
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

@SpringBootTest(classes = ValkeyQueryDemoApplication.class, webEnvironment = SpringBootTest.WebEnvironment.NONE)
@EnabledIfEnvironmentVariable(named = "VALKEY_INTEGRATION", matches = "true")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SkuDeserializationTest {

    @Autowired
    private GlideClient glideClient;

    @Autowired
    private SkuRepository skuRepository;

    private String runToken;

    @BeforeAll
    void setUp() throws Exception {
        recreateJsonIndex();
        runToken = "deserial" + UUID.randomUUID().toString().replace("-", "").substring(0, 8);
        System.out.println("Populating data with runToken: " + runToken);
        for (int price = 1; price <= 10; price++) {
            Sku sku = new Sku(
                    runToken + "-" + price,
                    "Advanced " + runToken + " Sku " + price,
                    price,
                    buildTags(price),
                    new Merchant(resolveMerchantName(price), resolveMerchantLevel(price)));
            skuRepository.save(sku);
            System.out.println("Saved: " + sku);
        }
    }

    @Test
    void printDeserializedRecords() {
        System.out.println("--- Testing deserialization of multiple records (list) ---");
        SkuQuery q = new SkuQuery();
        SearchCondition condition = q.title.contains(runToken).sortBy("price", true);

        List<Sku> records = skuRepository.list(condition);
        System.out.println("Found " + records.size() + " records:");
        records.forEach(sku -> System.out.println("Deserialized Sku: " + sku));

        System.out.println("\n--- Testing deserialization of paged records ---");
        Page<Sku> page = skuRepository.page(condition, 1, 5);
        System.out.println("Total: " + page.total());
        System.out.println("Page records size: " + page.records().size());
        page.records().forEach(sku -> System.out.println("Paged Deserialized Sku: " + sku));

        System.out.println("\n--- Testing deserialization of a single record (one) ---");
        String targetId = runToken + "-5";
        Sku one = skuRepository.one(q.id.eq(targetId));
        System.out.println("One record for id " + targetId + ": " + one);
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
