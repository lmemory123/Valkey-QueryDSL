package com.momao.valkey.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.momao.valkey.adapter.ValkeyClientRouting;
import com.momao.valkey.adapter.observability.ValkeyObservationInvoker;
import com.momao.valkey.adapter.observability.ValkeyUpdateMetricsRecorder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest(classes = ValkeyQueryDemoApplication.class, webEnvironment = SpringBootTest.WebEnvironment.NONE)
@EnabledIfEnvironmentVariable(named = "VALKEY_RW_INTEGRATION", matches = "true")
@TestPropertySource(properties = {
        "valkey.query.enabled=true",
        "valkey.query.mode=read_write_split",
        "valkey.query.read-preference=replica_preferred"
})
class ReadWriteSplitVectorIntegrationTests {

    @Autowired
    private ValkeyClientRouting clientRouting;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private ValkeyObservationInvoker observationInvoker;

    @Autowired
    private ValkeyUpdateMetricsRecorder updateMetricsRecorder;

    private VectorItemRepository repository;

    @DynamicPropertySource
    static void registerProperties(DynamicPropertyRegistry registry) {
        registry.add("valkey.query.read-write.write.nodes[0].host", () -> getenv("VALKEY_RW_WRITE_HOST", "localhost"));
        registry.add("valkey.query.read-write.write.nodes[0].port", () -> getenvInt("VALKEY_RW_WRITE_PORT", 6381));
        registry.add("valkey.query.read-write.read.nodes[0].host", () -> getenv("VALKEY_RW_READ_HOST", "localhost"));
        registry.add("valkey.query.read-write.read.nodes[0].port", () -> getenvInt("VALKEY_RW_READ_PORT", 6382));
        registry.add("valkey.query.username", () -> getenv("VALKEY_RW_USERNAME", ""));
        registry.add("valkey.query.password", () -> getenv("VALKEY_RW_PASSWORD", ""));
    }

    @BeforeEach
    void setUp() {
        repository = new VectorItemRepository(clientRouting, objectMapper, observationInvoker, updateMetricsRecorder);
        repository.checkAndCreateIndex();
        repository.save("rw-v-1", new VectorItem("rw-v-1", "camera one", "device", new float[]{1f, 0f, 0f}));
        repository.save("rw-v-2", new VectorItem("rw-v-2", "camera two", "device", new float[]{0.9f, 0.1f, 0f}));
    }

    @Test
    void vectorKnnWorksThroughReadWriteSplit() {
        VectorItemQuery q = new VectorItemQuery();
        List<VectorItem> results = repository.queryChain()
                .where(q.category.eq("device"))
                .knn(q.embedding, new float[]{1f, 0f, 0f}, 2)
                .list();

        assertEquals(List.of("rw-v-1", "rw-v-2"), results.stream().map(VectorItem::getId).toList());
    }

    private static String getenv(String name, String fallback) {
        String value = System.getenv(name);
        return value == null || value.isBlank() ? fallback : value;
    }

    private static int getenvInt(String name, int fallback) {
        String value = System.getenv(name);
        if (value == null || value.isBlank()) {
            return fallback;
        }
        return Integer.parseInt(value);
    }
}
