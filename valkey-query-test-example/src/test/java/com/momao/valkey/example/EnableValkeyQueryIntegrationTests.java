package com.momao.valkey.example;

import glide.api.GlideClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(classes = ValkeyQueryDemoApplication.class, webEnvironment = SpringBootTest.WebEnvironment.NONE)
@EnabledIfEnvironmentVariable(named = "VALKEY_PASSWORD", matches = ".+")
class EnableValkeyQueryIntegrationTests {

    @Autowired
    private GlideClient glideClient;

    @Autowired
    private SkuRepository skuRepository;

    @Test
    void autoConfigurationCreatesClientAndIndexes() throws Exception {
        assertNotNull(glideClient);
        assertNotNull(skuRepository);

        Object rawIndexes = glideClient.customCommand(new String[]{"FT._LIST"}).get();
        assertTrue(contains(rawIndexes, "idx:student"));
        assertTrue(contains(rawIndexes, "idx:sku"));
    }

    private boolean contains(Object values, String expected) {
        if (values instanceof Object[] array) {
            for (Object value : array) {
                if (expected.equals(String.valueOf(value))) {
                    return true;
                }
            }
            return false;
        }
        if (values instanceof Iterable<?> iterable) {
            for (Object value : iterable) {
                if (expected.equals(String.valueOf(value))) {
                    return true;
                }
            }
        }
        return false;
    }
}
