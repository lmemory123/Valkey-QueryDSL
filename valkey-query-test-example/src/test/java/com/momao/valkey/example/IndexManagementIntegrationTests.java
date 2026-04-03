package com.momao.valkey.example;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.momao.valkey.adapter.BaseValkeyRepository;
import com.momao.valkey.adapter.DefaultValkeyClientRouting;
import com.momao.valkey.adapter.IndexDiff;
import com.momao.valkey.adapter.ValkeyClientRouting;
import com.momao.valkey.annotation.StorageType;
import com.momao.valkey.autoconfigure.IndexManagementMode;
import com.momao.valkey.autoconfigure.ValkeyIndexManager;
import com.momao.valkey.autoconfigure.ValkeyQueryPackages;
import com.momao.valkey.autoconfigure.ValkeyQueryProperties;
import com.momao.valkey.core.metadata.IndexSchema;
import com.momao.valkey.core.metadata.SchemaField;
import glide.api.GlideClient;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(classes = ValkeyQueryDemoApplication.class, webEnvironment = SpringBootTest.WebEnvironment.NONE)
@EnabledIfEnvironmentVariable(named = "VALKEY_INTEGRATION", matches = "true")
class IndexManagementIntegrationTests {

    @Autowired
    private GlideClient glideClient;

    @Test
    void recreateModeDropsAndRebuildsIndex() throws Exception {
        String token = UUID.randomUUID().toString().replace("-", "");
        String indexName = "idx:ddl:recreate:" + token;
        String prefix = "ddl:recreate:" + token + ":";

        RecordingRouting routing = new RecordingRouting(glideClient);
        JsonSchemaRepository updatedRepository = new JsonSchemaRepository(routing, indexName, prefix, List.of(
                SchemaField.tag("id", ",", false),
                SchemaField.text("title", 1.0d, false, false),
                SchemaField.numeric("price", false)
        ));

        try {
            Object createResult = glideClient.customCommand(new String[]{
                    "FT.CREATE", indexName,
                    "ON", "JSON",
                    "PREFIX", "1", prefix,
                    "SCHEMA",
                    "$.id", "AS", "id", "TAG", "SEPARATOR", ",",
                    "$.title", "AS", "title", "TEXT", "WITHSUFFIXTRIE"
            }).get();
            System.out.println("[index-recreate] baseline-create=" + createResult);
            awaitDiff(updatedRepository, "price");

            ValkeyQueryProperties properties = new ValkeyQueryProperties();
            properties.getIndexManagement().setMode(IndexManagementMode.RECREATE);
            ValkeyIndexManager indexManager = new ValkeyIndexManager(routing, new ValkeyQueryPackages(List.of()), properties, null);

            routing.clear();
            indexManager.applyIndexManagement(updatedRepository);

            List<String> dropCommand = routing.findFirst("FT.DROPINDEX");
            List<String> createCommand = routing.findFirst("FT.CREATE");
            System.out.println("[index-recreate] drop=" + String.join(" ", dropCommand));
            System.out.println("[index-recreate] create=" + String.join(" ", createCommand));

            assertTrue(dropCommand.contains(indexName));
            assertTrue(createCommand.contains(indexName));
            assertTrue(createCommand.contains("price"));
            assertTrue(createCommand.contains("NUMERIC"));
            awaitEmptyDiff(updatedRepository);
        } finally {
            try {
                glideClient.customCommand(new String[]{"FT.DROPINDEX", indexName}).get();
            } catch (Exception ignored) {
            }
        }
    }

    private void awaitDiff(JsonSchemaRepository repository, String missingFieldName) {
        long deadline = System.currentTimeMillis() + 5_000L;
        String lastObserved = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                IndexDiff diff = repository.inspectIndexDiff();
                if (diff.items().stream().anyMatch(item -> missingFieldName.equals(item.target()))) {
                    return;
                }
                lastObserved = diff.summary();
            } catch (Exception exception) {
                lastObserved = exception.getMessage();
            }
            try {
                Thread.sleep(100L);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("等待索引创建结果时线程被中断", exception);
            }
        }
        throw new IllegalStateException("基线索引未出现预期 diff: " + lastObserved);
    }

    private void awaitEmptyDiff(JsonSchemaRepository repository) {
        long deadline = System.currentTimeMillis() + 5_000L;
        String lastObserved = null;
        while (System.currentTimeMillis() < deadline) {
            try {
                IndexDiff diff = repository.inspectIndexDiff();
                if (diff.isEmpty()) {
                    return;
                }
                lastObserved = diff.summary();
            } catch (Exception exception) {
                lastObserved = exception.getMessage();
            }
            try {
                Thread.sleep(100L);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("等待索引创建结果时线程被中断", exception);
            }
        }
        throw new IllegalStateException("索引 diff 未在预期时间内收敛为空: " + lastObserved);
    }

    @Test
    void noneModeSkipsIndexInspectionCompletely() {
        String token = UUID.randomUUID().toString().replace("-", "");
        String indexName = "idx:ddl:none:" + token;
        String prefix = "ddl:none:" + token + ":";

        RecordingRouting routing = new RecordingRouting(glideClient);
        JsonSchemaRepository repository = new JsonSchemaRepository(routing, indexName, prefix, List.of(
                SchemaField.tag("id", ",", false),
                SchemaField.text("title", 1.0d, false, false)
        ));

        ValkeyQueryProperties properties = new ValkeyQueryProperties();
        properties.getIndexManagement().setMode(IndexManagementMode.NONE);
        ValkeyIndexManager indexManager = new ValkeyIndexManager(routing, new ValkeyQueryPackages(List.of()), properties, null);

        routing.clear();
        indexManager.applyIndexManagement(repository);

        System.out.println("[index-none] commands=" + routing.indexCommands());
        assertEquals(List.of(), routing.indexCommands());
    }

    @Test
    void validateModeFailsFastWhenRealIndexDiffExists() throws Exception {
        String token = UUID.randomUUID().toString().replace("-", "");
        String indexName = "idx:ddl:validate:" + token;
        String prefix = "ddl:validate:" + token + ":";

        RecordingRouting routing = new RecordingRouting(glideClient);
        JsonSchemaRepository repository = new JsonSchemaRepository(routing, indexName, prefix, List.of(
                SchemaField.tag("id", ",", false),
                SchemaField.text("title", 1.0d, false, false),
                SchemaField.numeric("price", false)
        ));

        try {
            glideClient.customCommand(new String[]{
                    "FT.CREATE", indexName,
                    "ON", "JSON",
                    "PREFIX", "1", prefix,
                    "SCHEMA",
                    "$.id", "AS", "id", "TAG", "SEPARATOR", ",",
                    "$.title", "AS", "title", "TEXT", "WITHSUFFIXTRIE"
            }).get();
            awaitDiff(repository, "price");

            ValkeyQueryProperties properties = new ValkeyQueryProperties();
            properties.getIndexManagement().setMode(IndexManagementMode.VALIDATE);
            ValkeyIndexManager indexManager = new ValkeyIndexManager(routing, new ValkeyQueryPackages(List.of()), properties, null);

            IllegalStateException error = assertThrows(IllegalStateException.class, () -> indexManager.applyIndexManagement(repository));
            assertEquals("索引不一致，VALIDATE 模式已阻止启动，请检查 schema 或手动重建索引。", error.getMessage());
        } finally {
            try {
                glideClient.customCommand(new String[]{"FT.DROPINDEX", indexName}).get();
            } catch (Exception ignored) {
            }
        }
    }

    private static final class JsonSchemaRepository extends BaseValkeyRepository<Object> {

        private JsonSchemaRepository(ValkeyClientRouting routing, String indexName, String prefix, List<SchemaField> fields) {
            super(new IndexSchema(indexName, StorageType.JSON, List.of(prefix), fields), routing, Object.class, new ObjectMapper().findAndRegisterModules());
        }
    }

    private static final class RecordingRouting implements ValkeyClientRouting {

        private final DefaultValkeyClientRouting delegate;
        private final List<List<String>> indexCommands = new ArrayList<>();

        private RecordingRouting(GlideClient glideClient) {
            this.delegate = new DefaultValkeyClientRouting(glideClient);
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
                    .orElseThrow(() -> new IllegalStateException("未找到索引命令: " + commandName + " commands=" + indexCommands));
        }

        private List<List<String>> indexCommands() {
            return indexCommands;
        }

        private void clear() {
            indexCommands.clear();
        }
    }
}
