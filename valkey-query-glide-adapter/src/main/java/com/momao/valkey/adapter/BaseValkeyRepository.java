package com.momao.valkey.adapter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.momao.valkey.core.Page;
import com.momao.valkey.core.SearchCondition;
import com.momao.valkey.core.SearchResult;
import com.momao.valkey.core.ValkeyRepository;
import com.momao.valkey.core.metadata.IndexSchema;
import com.momao.valkey.core.metadata.SchemaField;
import glide.api.GlideClient;
import glide.api.commands.servermodules.FT;
import glide.api.models.commands.FT.FTCreateOptions;
import glide.api.models.commands.FT.FTSearchOptions;
import glide.api.models.commands.FT.FTCreateOptions.FieldInfo;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public abstract class BaseValkeyRepository<T> implements ValkeyRepository<T> {

    private static final int DEFAULT_LIST_LIMIT = 10_000;

    protected final IndexSchema schema;

    protected final GlideClient client;

    protected final Class<T> entityClass;

    protected final ObjectMapper objectMapper;

    private final ValkeyEntityOperations<T> entityOperations;

    private final ValkeyResultMapper<T> resultMapper;

    protected BaseValkeyRepository(IndexSchema schema, Class<T> entityClass) {
        this(schema, null, entityClass, null);
    }

    protected BaseValkeyRepository(IndexSchema schema, GlideClient client, Class<T> entityClass, ObjectMapper objectMapper) {
        this.schema = schema;
        this.client = client;
        this.entityClass = entityClass;
        this.objectMapper = objectMapper == null ? new ObjectMapper().findAndRegisterModules() : objectMapper;
        this.entityOperations = ValkeyEntityOperationsFactory.create(schema.storageType());
        this.resultMapper = new ValkeyResultMapper<>(schema, entityClass, this.objectMapper, entityOperations);
    }

    public String checkAndCreateIndex() {
        GlideClient glideClient = requireClient();
        if (indexExists(glideClient, schema.indexName())) {
            if (indexMatchesSchema(glideClient)) {
                return "OK";
            }
            recreateIndex(glideClient);
        }
        try {
            return FT.create(glideClient, schema.indexName(), buildFieldInfos(), buildCreateOptions()).get();
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("创建索引时线程被中断: " + schema.indexName(), exception);
        } catch (Exception exception) {
            throw new IllegalStateException("创建索引失败: " + schema.indexName(), exception);
        }
    }

    public void save(String id, T entity) {
        GlideClient glideClient = requireClient();
        String key = buildKey(id);
        try {
            entityOperations.save(glideClient, key, entity, objectMapper);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("保存实体时线程被中断: " + key, exception);
        } catch (Exception exception) {
            throw new IllegalStateException("保存实体失败: " + key, exception);
        }
    }

    public SearchResult<T> search(SearchCondition condition) {
        return executeSearch(condition, 0, DEFAULT_LIST_LIMIT);
    }

    public List<T> list(SearchCondition condition) {
        return page(condition, 0, DEFAULT_LIST_LIMIT).records();
    }

    public Page<T> page(SearchCondition condition, int offset, int limit) {
        SearchResult<T> result = executeSearch(condition, offset, limit);
        return new Page<>(result.total(), result.records());
    }

    public T one(SearchCondition condition) {
        List<T> records = page(condition, 0, 1).records();
        return records.isEmpty() ? null : records.get(0);
    }

    public long count(SearchCondition condition) {
        GlideClient glideClient = requireClient();
        try {
            String[] command = buildCountCommand(condition);
            System.out.println("[valkey-query] command=" + String.join(" ", command) + " [mode=custom-count-fallback]");
            Object response = glideClient.customCommand(command).get();
            if (response instanceof Object[] values) {
                return values.length == 0 ? 0L : toLong(values[0]);
            }
            return response == null ? 0L : toLong(response);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("执行检索时线程被中断: " + schema.indexName(), exception);
        } catch (Exception exception) {
            throw new IllegalStateException("执行 count 查询失败: " + schema.indexName(), exception);
        }
    }

    public String getIndexName() {
        return schema.indexName();
    }

    public String getPrefix() {
        return schema.prefix();
    }

    public List<String> getPrefixes() {
        return new ArrayList<>(schema.prefixes());
    }

    public List<SchemaField> getFields() {
        return new ArrayList<>(schema.fields());
    }

    protected String buildKey(String id) {
        return schema.prefix() + id;
    }

    protected T convertStoredFields(Map<String, ?> fields) {
        try {
            return entityOperations.readEntity(objectMapper, entityClass, fields);
        } catch (Exception exception) {
            throw new IllegalStateException("结果映射失败: " + entityClass.getName(), exception);
        }
    }

    protected Map<String, String> toStringMap(T entity) {
        return entityOperations.toStorageFields(entity, objectMapper);
    }

    protected SearchResult<T> mapSearchResponse(Object[] response) {
        return resultMapper.mapSearchResponse(response);
    }

    protected GlideClient requireClient() {
        if (client == null) {
            throw new IllegalStateException("GlideClient 未注入，无法执行真实 Valkey I/O");
        }
        return client;
    }

    private long toLong(Object value) {
        if (value instanceof Number number) {
            return number.longValue();
        }
        return Long.parseLong(String.valueOf(value));
    }

    private SearchResult<T> executeSearch(SearchCondition condition, int offset, int limit) {
        GlideClient glideClient = requireClient();
        try {
            if (condition != null && condition.hasSort()) {
                String[] command = buildSearchCommand(condition, offset, limit);
                logSearchCommand(command);
                return mapSearchResponse(asObjectArray(glideClient.customCommand(command).get()));
            }

            FTSearchOptions options = createSearchOptions(offset, limit);
            logSearchInvocation(condition, options);
            Object[] response = FT.search(glideClient, schema.indexName(), buildQuery(condition), options).get();
            return mapSearchResponse(response);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("执行检索时线程被中断: " + schema.indexName(), exception);
        } catch (Exception exception) {
            throw new IllegalStateException("执行检索与结果映射失败: " + schema.indexName(), exception);
        }
    }

    private FTSearchOptions createSearchOptions(int offset, int limit) {
        int safeOffset = Math.max(0, offset);
        int safeLimit = Math.max(0, limit);
        FTSearchOptions.FTSearchOptionsBuilder builder = FTSearchOptions.builder();
        builder.limit(safeOffset, safeLimit);
        return builder.build();
    }

    private void logSearchInvocation(SearchCondition condition, FTSearchOptions options) {
        List<String> tokens = new ArrayList<>();
        tokens.add("FT.SEARCH");
        tokens.add(schema.indexName());
        tokens.add(buildQuery(condition));
        for (Object argument : options.toArgs()) {
            tokens.add(String.valueOf(argument));
        }
        if (condition != null && condition.hasSort()) {
            tokens.add("[client-sort]");
            tokens.add(condition.sortField());
            tokens.add(condition.sortAscending() ? "ASC" : "DESC");
        }
        tokens.add("[mode=sdk-search]");
        System.out.println("[valkey-query] command=" + String.join(" ", tokens));
    }

    private void logSearchCommand(String[] command) {
        System.out.println("[valkey-query] command=" + String.join(" ", command) + " [mode=custom-search-sort]");
    }

    private String buildQuery(SearchCondition condition) {
        return condition == null || condition.build().isBlank() ? "*" : condition.build();
    }

    private boolean indexExists(GlideClient glideClient, String indexName) {
        try {
            Object response = glideClient.customCommand(new String[]{"FT._LIST"}).get();
            if (response instanceof Object[] values) {
                for (Object value : values) {
                    if (indexName.equals(asString(value))) {
                        return true;
                    }
                }
                return false;
            }
            if (response instanceof Iterable<?> values) {
                for (Object value : values) {
                    if (indexName.equals(asString(value))) {
                        return true;
                    }
                }
            }
            return false;
        } catch (Exception exception) {
            return false;
        }
    }

    private boolean indexMatchesSchema(GlideClient glideClient) {
        try {
            Object response = glideClient.customCommand(new String[]{"FT.INFO", schema.indexName()}).get();
            ExistingIndexInfo indexInfo = ExistingIndexInfo.fromResponse(response);
            return indexInfo.matches(schema, entityOperations.dataType());
        } catch (Exception exception) {
            return false;
        }
    }

    private void recreateIndex(GlideClient glideClient) {
        try {
            System.out.println("[valkey-query] index schema mismatch, recreating index=" + schema.indexName());
            glideClient.customCommand(new String[]{"FT.DROPINDEX", schema.indexName()}).get();
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("重建索引时线程被中断: " + schema.indexName(), exception);
        } catch (Exception exception) {
            throw new IllegalStateException("删除旧索引失败: " + schema.indexName(), exception);
        }
    }

    private FTCreateOptions buildCreateOptions() {
        FTCreateOptions.FTCreateOptionsBuilder builder = FTCreateOptions.builder()
                .dataType(entityOperations.dataType());
        if (!schema.prefixes().isEmpty()) {
            builder.prefixes(schema.prefixes().toArray(String[]::new));
        }
        return builder.build();
    }

    private FieldInfo[] buildFieldInfos() {
        return schema.fields().stream()
                .map(entityOperations::toFieldInfo)
                .toArray(FieldInfo[]::new);
    }

    protected String[] buildSearchCommand(SearchCondition condition, int offset, int limit) {
        List<String> command = new ArrayList<>();
        command.add("FT.SEARCH");
        command.add(schema.indexName());
        command.add(buildQuery(condition));
        if (condition != null && condition.hasSort()) {
            command.add("SORTBY");
            command.add(condition.sortField());
            command.add(condition.sortAscending() ? "ASC" : "DESC");
        }
        command.add("LIMIT");
        command.add(Integer.toString(Math.max(0, offset)));
        command.add(Integer.toString(Math.max(0, limit)));
        return command.toArray(String[]::new);
    }

    private String[] buildCountCommand(SearchCondition condition) {
        return new String[]{
                "FT.SEARCH",
                schema.indexName(),
                buildQuery(condition),
                "LIMIT",
                "0",
                "0"
        };
    }

    private Object[] asObjectArray(Object response) {
        if (response instanceof Object[] values) {
            return values;
        }
        if (response instanceof List<?> values) {
            return values.toArray();
        }
        return response == null ? new Object[0] : new Object[]{response};
    }

    private String asString(Object value) {
        return String.valueOf(value);
    }
}
