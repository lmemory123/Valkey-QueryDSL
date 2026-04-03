package com.momao.valkey.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.momao.valkey.adapter.BaseValkeyRepository;
import com.momao.valkey.adapter.IndexDiff;
import com.momao.valkey.adapter.ValkeyClientRouting;
import com.momao.valkey.annotation.StorageType;
import com.momao.valkey.autoconfigure.IndexManagementMode;
import com.momao.valkey.autoconfigure.ValkeyIndexManager;
import com.momao.valkey.autoconfigure.ValkeyQueryPackages;
import com.momao.valkey.autoconfigure.ValkeyQueryProperties;
import com.momao.valkey.core.metadata.IndexSchema;
import com.momao.valkey.core.metadata.SchemaField;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.test.context.TestPropertySource;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(classes = ValkeyQueryDemoApplication.class, webEnvironment = SpringBootTest.WebEnvironment.NONE)
@EnabledIfEnvironmentVariable(named = "VALKEY_CLUSTER_INTEGRATION", matches = "true")
@TestPropertySource(properties = {
        "valkey.query.enabled=true",
        "valkey.query.mode=cluster"
})
class ClusterIndexManagementIntegrationTests {

    @Autowired
    private ValkeyClientRouting clientRouting;

    @DynamicPropertySource
    static void registerProperties(DynamicPropertyRegistry registry) {
        registry.add("valkey.query.cluster.nodes[0].host", () -> getenv("VALKEY_CLUSTER_HOST", "localhost"));
        registry.add("valkey.query.cluster.nodes[0].port", () -> getenvInt("VALKEY_CLUSTER_PORT", 8000));
    }

    @Test
    void noneModeSkipsIndexInspectionThroughCluster() {
        String token = UUID.randomUUID().toString().replace("-", "");
        RecordingRouting routing = new RecordingRouting(clientRouting);
        JsonSchemaRepository repository = new JsonSchemaRepository(
                routing,
                "idx:ddl:cluster:none:" + token,
                "ddl:cluster:none:" + token + ":",
                List.of(
                        SchemaField.tag("id", ",", false),
                        SchemaField.text("title", 1.0d, false, false)
                )
        );

        ValkeyQueryProperties properties = new ValkeyQueryProperties();
        properties.getIndexManagement().setMode(IndexManagementMode.NONE);
        ValkeyIndexManager indexManager = new ValkeyIndexManager(routing, new ValkeyQueryPackages(List.of()), properties, null);

        routing.clear();
        indexManager.applyIndexManagement(repository);

        assertEquals(List.of(), routing.indexCommands());
    }

    @Test
    void recreateModeDropsAndRebuildsIndexThroughCluster() throws Exception {
        String token = UUID.randomUUID().toString().replace("-", "");
        String indexName = "idx:ddl:cluster:recreate:" + token;
        String prefix = "ddl:cluster:recreate:" + token + ":";

        RecordingRouting routing = new RecordingRouting(clientRouting);
        JsonSchemaRepository repository = new JsonSchemaRepository(
                routing,
                indexName,
                prefix,
                List.of(
                        SchemaField.tag("id", ",", false),
                        SchemaField.text("title", 1.0d, false, false),
                        SchemaField.numeric("price", false)
                )
        );

        try {
            routing.executeIndex(new String[]{
                    "FT.CREATE", indexName,
                    "ON", "JSON",
                    "PREFIX", "1", prefix,
                    "SCHEMA",
                    "$.id", "AS", "id", "TAG", "SEPARATOR", ",",
                    "$.title", "AS", "title", "TEXT", "WITHSUFFIXTRIE"
            });

            ValkeyQueryProperties properties = new ValkeyQueryProperties();
            properties.getIndexManagement().setMode(IndexManagementMode.RECREATE);
            ValkeyIndexManager indexManager = new ValkeyIndexManager(routing, new ValkeyQueryPackages(List.of()), properties, null);

            routing.clear();
            indexManager.applyIndexManagement(repository);

            List<String> dropCommand = routing.findFirst("FT.DROPINDEX");
            List<String> createCommand = routing.findFirst("FT.CREATE");
            assertTrue(dropCommand.contains(indexName));
            assertTrue(createCommand.contains(indexName));
            assertTrue(createCommand.contains("price"));
            assertTrue(createCommand.contains("NUMERIC"));
        } finally {
            try {
                routing.executeIndex(new String[]{"FT.DROPINDEX", indexName});
            } catch (Exception ignored) {
            }
        }
    }

    @Test
    void validateModeFailsFastThroughCluster() throws Exception {
        String token = UUID.randomUUID().toString().replace("-", "");
        String indexName = "idx:ddl:cluster:validate:" + token;
        String prefix = "ddl:cluster:validate:" + token + ":";

        RecordingRouting routing = new RecordingRouting(clientRouting);
        JsonSchemaRepository repository = new JsonSchemaRepository(
                routing,
                indexName,
                prefix,
                List.of(
                        SchemaField.tag("id", ",", false),
                        SchemaField.text("title", 1.0d, false, false),
                        SchemaField.numeric("price", false)
                )
        );

        try {
            routing.executeIndex(new String[]{
                    "FT.CREATE", indexName,
                    "ON", "JSON",
                    "PREFIX", "1", prefix,
                    "SCHEMA",
                    "$.id", "AS", "id", "TAG", "SEPARATOR", ",",
                    "$.title", "AS", "title", "TEXT", "WITHSUFFIXTRIE"
            });
            awaitDiff(repository, "price");

            ValkeyQueryProperties properties = new ValkeyQueryProperties();
            properties.getIndexManagement().setMode(IndexManagementMode.VALIDATE);
            ValkeyIndexManager indexManager = new ValkeyIndexManager(routing, new ValkeyQueryPackages(List.of()), properties, null);

            IllegalStateException error = assertThrows(IllegalStateException.class, () -> indexManager.applyIndexManagement(repository));
            assertEquals("索引不一致，VALIDATE 模式已阻止启动，请检查 schema 或手动重建索引。", error.getMessage());
        } finally {
            try {
                routing.executeIndex(new String[]{"FT.DROPINDEX", indexName});
            } catch (Exception ignored) {
            }
        }
    }

    private void awaitDiff(JsonSchemaRepository repository, String missingFieldName) throws Exception {
        long deadline = System.currentTimeMillis() + 5_000L;
        while (System.currentTimeMillis() < deadline) {
            IndexDiff diff = repository.inspectIndexDiff();
            if (diff.items().stream().anyMatch(item -> missingFieldName.equals(item.target()))) {
                return;
            }
            Thread.sleep(100L);
        }
        throw new IllegalStateException("cluster 下索引 diff 未按预期出现: " + missingFieldName);
    }

    private static final class JsonSchemaRepository extends BaseValkeyRepository<Object> {

        private JsonSchemaRepository(ValkeyClientRouting routing, String indexName, String prefix, List<SchemaField> fields) {
            super(new IndexSchema(indexName, StorageType.JSON, List.of(prefix), fields), routing, Object.class, new ObjectMapper().findAndRegisterModules());
        }
    }

    private static final class RecordingRouting implements ValkeyClientRouting {

        private final ValkeyClientRouting delegate;
        private final List<List<String>> indexCommands = new ArrayList<>();

        private RecordingRouting(ValkeyClientRouting delegate) {
            this.delegate = delegate;
        }

        @Override
        public Object executeWrite(String[] command) throws Exception {
            return delegate.executeWrite(command);
        }

        @Override
        public Object executeRead(String[] command) throws Exception {
            return delegate.executeRead(command);
        }

        @Override
        public Object executeIndex(String[] command) throws Exception {
            indexCommands.add(List.copyOf(java.util.Arrays.asList(command.clone())));
            return delegate.executeIndex(command);
        }

        private List<String> findFirst(String commandName) {
            return indexCommands.stream()
                    .filter(command -> !command.isEmpty() && commandName.equals(command.get(0)))
                    .findFirst()
                    .orElseThrow();
        }

        private List<List<String>> indexCommands() {
            return indexCommands;
        }

        private void clear() {
            indexCommands.clear();
        }
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
