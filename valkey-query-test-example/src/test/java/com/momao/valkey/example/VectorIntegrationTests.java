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

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest(classes = ValkeyQueryDemoApplication.class, webEnvironment = SpringBootTest.WebEnvironment.NONE)
@EnabledIfEnvironmentVariable(named = "VALKEY_INTEGRATION", matches = "true")
class VectorIntegrationTests {

    @Autowired
    private ValkeyClientRouting clientRouting;

    @Autowired
    private ObjectMapper objectMapper;

    @Autowired
    private ValkeyObservationInvoker observationInvoker;

    @Autowired
    private ValkeyUpdateMetricsRecorder updateMetricsRecorder;

    private VectorItemRepository repository;

    @BeforeEach
    void setUp() {
        repository = new VectorItemRepository(clientRouting, objectMapper, observationInvoker, updateMetricsRecorder);
        repository.checkAndCreateIndex();
        repository.save("v-1", new VectorItem("v-1", "apple phone", "device", new float[]{1f, 0f, 0f}));
        repository.save("v-2", new VectorItem("v-2", "orange fruit", "food", new float[]{0f, 1f, 0f}));
        repository.save("v-3", new VectorItem("v-3", "apple tablet", "device", new float[]{0.9f, 0.1f, 0f}));
    }

    @Test
    void knnSearchWorksAgainstRealStandaloneValkey() {
        VectorItemQuery q = new VectorItemQuery();
        List<VectorItem> results = repository.queryChain()
                .where(q.category.eq("device"))
                .knn(q.embedding, new float[]{1f, 0f, 0f}, 2)
                .list();

        assertEquals(List.of("v-1", "v-3"), results.stream().map(VectorItem::getId).toList());
        assertArrayEquals(new float[]{1f, 0f, 0f}, results.get(0).getEmbedding());
    }
}
