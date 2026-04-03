package com.momao.valkey.example;

import com.momao.valkey.core.Page;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

@SpringBootTest(classes = ValkeyQueryDemoApplication.class, webEnvironment = SpringBootTest.WebEnvironment.NONE)
@EnabledIfEnvironmentVariable(named = "VALKEY_INTEGRATION", matches = "true")
class ProjectionIntegrationTests {

    record SkuProjectionView(String id, String title, String merchantName) {
    }

    @Autowired
    private SkuRepository skuRepository;

    @Test
    void selectReturnsProjectedJsonFieldsFromRealValkey() throws Exception {
        skuRepository.checkAndCreateIndex();

        String token = "proj" + UUID.randomUUID().toString().replace("-", "");
        Sku sku = new Sku(
                token,
                "Projection-" + token,
                4099,
                List.of("HOT", "PROJ"),
                new Merchant("Projection Merchant", "VIP"));
        skuRepository.save(sku);

        SkuQuery q = new SkuQuery();
        awaitIndexed(token, q);

        Sku projected = skuRepository.queryChain()
                .where(q.id.eq(token))
                .select("title", "merchant_name")
                .one();

        assertNotNull(projected);
        assertEquals(token, projected.getId());
        assertEquals("Projection-" + token, projected.getTitle());
        assertNull(projected.getPrice());
        assertNull(projected.getTags());
        assertNotNull(projected.getMerchant());
        assertEquals("Projection Merchant", projected.getMerchant().getName());
        assertNull(projected.getMerchant().getLevel());
    }

    @Test
    void typedProjectionMapperWorksAgainstRealValkey() throws Exception {
        skuRepository.checkAndCreateIndex();

        String token = "projmap" + UUID.randomUUID().toString().replace("-", "");
        Sku first = new Sku(token + "-1", "ProjectionMap-" + token + "-1", 5101, List.of("PROJMAP", token), new Merchant("PM-1", "VIP"));
        Sku second = new Sku(token + "-2", "ProjectionMap-" + token + "-2", 5102, List.of("PROJMAP", token), new Merchant("PM-2", "VIP"));
        skuRepository.save(first);
        skuRepository.save(second);

        SkuQuery q = new SkuQuery();
        awaitIndexed(token + "-1", q);
        awaitIndexed(token + "-2", q);

        List<SkuProjectionView> rows = skuRepository.queryChain()
                .where(q.tags.contains(token))
                .select("title", "merchant_name")
                .orderByAsc("title")
                .list(sku -> new SkuProjectionView(
                        sku.getId(),
                        sku.getTitle(),
                        sku.getMerchant() == null ? null : sku.getMerchant().getName()
                ));

        assertEquals(2, rows.size());
        assertEquals("ProjectionMap-" + token + "-1", rows.get(0).title());
        assertEquals("PM-1", rows.get(0).merchantName());

        Page<SkuProjectionView> page = skuRepository.queryChain()
                .where(q.tags.contains(token))
                .select("title", "merchant_name")
                .orderByAsc("title")
                .page(0, 1, sku -> new SkuProjectionView(
                        sku.getId(),
                        sku.getTitle(),
                        sku.getMerchant() == null ? null : sku.getMerchant().getName()
                ));

        assertEquals(2L, page.total());
        assertEquals(1, page.records().size());
        assertFalse(page.records().get(0).id().isBlank());
    }

    private void awaitIndexed(String token, SkuQuery q) throws Exception {
        long deadline = System.currentTimeMillis() + 5_000L;
        while (System.currentTimeMillis() < deadline) {
            long total = skuRepository.queryChain()
                    .where(q.id.eq(token))
                    .count();
            if (total > 0) {
                return;
            }
            Thread.sleep(100L);
        }
        throw new IllegalStateException("projection 测试文档未在超时时间内进入索引: " + token);
    }
}
