package com.momao.valkey.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.momao.valkey.adapter.ValkeyClientRouting;
import com.momao.valkey.adapter.observability.ValkeyObservationInvoker;
import com.momao.valkey.adapter.observability.ValkeyUpdateMetricsRecorder;
import com.momao.valkey.core.exception.ValkeyQueryExecutionException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(classes = ValkeyQueryDemoApplication.class, webEnvironment = SpringBootTest.WebEnvironment.NONE)
@EnabledIfEnvironmentVariable(named = "VALKEY_CLUSTER_INTEGRATION", matches = "true")
@TestPropertySource(properties = {
        "valkey.query.enabled=true",
        "valkey.query.mode=cluster"
})
class ClusterVectorIntegrationTests {

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
        registry.add("valkey.query.cluster.nodes[0].host", () -> getenv("VALKEY_CLUSTER_HOST", "localhost"));
        registry.add("valkey.query.cluster.nodes[0].port", () -> getenvInt("VALKEY_CLUSTER_PORT", 8000));
    }

    @BeforeEach
    void setUp() {
        repository = new VectorItemRepository(clientRouting, objectMapper, observationInvoker, updateMetricsRecorder);
        repository.checkAndCreateIndex();
        repository.save("c-v-1", new VectorItem("c-v-1", "speaker one", "device", new float[]{1f, 0f, 0f}));
    }

    @Test
    void vectorKnnIsRejectedAgainstRealClusterValkey() {
        VectorItemQuery q = new VectorItemQuery();
        ValkeyQueryExecutionException error = assertThrows(
                ValkeyQueryExecutionException.class,
                () -> repository.queryChain()
                        .where(q.category.eq("device"))
                        .knn(q.embedding, new float[]{1f, 0f, 0f}, 1)
                        .list()
        );
        assertTrue(error.getMessage().contains("不支持向量 KNN"));
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
