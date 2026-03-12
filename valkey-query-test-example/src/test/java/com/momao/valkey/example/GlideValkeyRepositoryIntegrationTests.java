package com.momao.valkey.example;

import com.momao.valkey.core.Page;
import glide.api.GlideClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@SpringBootTest(classes = ValkeyQueryDemoApplication.class, webEnvironment = SpringBootTest.WebEnvironment.NONE)
@EnabledIfEnvironmentVariable(named = "VALKEY_PASSWORD", matches = ".+")
class GlideValkeyRepositoryIntegrationTests {

    @Autowired
    private GlideClient glideClient;

    @Autowired
    private SkuRepository skuRepository;

    @Test
    void savesJsonAndQueriesThroughValkey() throws Exception {
        String token = "it" + UUID.randomUUID().toString().replace("-", "");
        Sku sku = new Sku(
                token,
                "IntegrationPro" + token,
                2999,
                List.of("HOT", "NEW"),
                new Merchant("Apple Flagship", "S"));

        String ping = glideClient.ping().get();
        skuRepository.save(sku);

        SkuQuery q = new SkuQuery();
        awaitIndexed(q, token);
        Sku loaded = skuRepository.findById(token);
        Sku matched = skuRepository.queryChain()
            .where(q.title.eq("IntegrationPro" + token).and(
                q.merchant.name.eq("Apple Flagship"
                )
            ))
            .one();
        Page<Sku> result = skuRepository.queryChain()
            .where(q.id.eq(token))
            .and(q.tags.contains("HOT"))
            .orderByAsc("price")
            .page(0, 10);
        long total = skuRepository.queryChain()
            .where(q.id.eq(token))
            .count();

        System.out.println("[integration] ping=" + ping);
        System.out.println("[integration] key=" + skuRepository.getPrefix() + token);
        System.out.println("[integration] query=" + q.id.eq(token).and(q.tags.contains("HOT")).build());
        System.out.println("[integration] total=" + total);
        System.out.println("[integration] records=" + result.records());

        assertEquals("PONG", ping);
        assertEquals(token, loaded.getId());
        assertEquals("IntegrationPro" + token, loaded.getTitle());
        assertNotNull(matched);
        assertEquals(token, matched.getId());
        assertEquals(1L, total);
        assertTrue(result.total() >= 1);
        assertTrue(result.records().stream().anyMatch(item -> token.equals(item.getId())));
        assertEquals(2999, matched.getPrice());
        assertEquals(List.of("HOT", "NEW"), matched.getTags());
        assertEquals("Apple Flagship", matched.getMerchant().getName());
    }

    private void awaitIndexed(SkuQuery q, String token) throws Exception {
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
        fail("文档已写入 JSON，但在超时时间内未进入搜索索引: " + token);
    }
}
