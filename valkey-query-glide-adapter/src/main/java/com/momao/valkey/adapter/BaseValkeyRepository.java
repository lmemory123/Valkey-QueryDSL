package com.momao.valkey.adapter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.momao.valkey.annotation.FieldType;
import com.momao.valkey.adapter.observability.ValkeyObservationInvoker;
import com.momao.valkey.adapter.observability.ValkeyUpdateMetricsRecorder;
import com.momao.valkey.core.BulkMode;
import com.momao.valkey.core.BulkSaveItem;
import com.momao.valkey.core.BulkUpdateItem;
import com.momao.valkey.core.BulkWriteItemResult;
import com.momao.valkey.core.BulkWriteOptions;
import com.momao.valkey.core.BulkWriteResult;
import com.momao.valkey.core.AggregateReducer;
import com.momao.valkey.core.AggregateReducerKind;
import com.momao.valkey.core.AggregateApply;
import com.momao.valkey.core.AggregateRequest;
import com.momao.valkey.core.AggregateResult;
import com.momao.valkey.core.AggregateRow;
import com.momao.valkey.core.Page;
import com.momao.valkey.core.UpdateOperation;
import com.momao.valkey.core.SearchCondition;
import com.momao.valkey.core.SearchPredicate;
import com.momao.valkey.core.SearchResult;
import com.momao.valkey.core.ValkeyRepository;
import com.momao.valkey.core.VectorQuery;
import com.momao.valkey.core.exception.ValkeyErrorCode;
import com.momao.valkey.core.exception.ValkeyConfigurationException;
import com.momao.valkey.core.exception.ValkeyIndexException;
import com.momao.valkey.core.exception.ValkeyQueryException;
import com.momao.valkey.core.exception.ValkeyQueryExecutionException;
import com.momao.valkey.core.exception.ValkeyResultMappingException;
import com.momao.valkey.core.metadata.IndexSchema;
import com.momao.valkey.core.metadata.SchemaField;
import glide.api.GlideClient;
import glide.api.models.GlideString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;

public abstract class BaseValkeyRepository<T> implements ValkeyRepository<T> {

    private static final int DEFAULT_LIST_LIMIT = 1_000;
    private static final int MAX_CLUSTER_MERGE_WINDOW = 2_000;
    private static final Logger log = LoggerFactory.getLogger(BaseValkeyRepository.class);

    protected final IndexSchema schema;

    protected final ValkeyClientRouting clientRouting;

    protected final Class<T> entityClass;

    protected final ObjectMapper objectMapper;

    protected final ValkeyObservationInvoker observationInvoker;

    protected final ValkeyUpdateMetricsRecorder updateMetricsRecorder;

    private final ValkeyEntityOperations<T> entityOperations;

    private final ValkeyResultMapper<T> resultMapper;

    protected BaseValkeyRepository(IndexSchema schema, Class<T> entityClass) {
        this(schema, (ValkeyClientRouting) null, entityClass, null, ValkeyObservationInvoker.noop(), ValkeyUpdateMetricsRecorder.NOOP);
    }

    protected BaseValkeyRepository(IndexSchema schema, GlideClient client, Class<T> entityClass, ObjectMapper objectMapper) {
        this(schema, client == null ? null : new DefaultValkeyClientRouting(client), entityClass, objectMapper, ValkeyObservationInvoker.noop(), ValkeyUpdateMetricsRecorder.NOOP);
    }

    protected BaseValkeyRepository(IndexSchema schema, ValkeyClientRouting clientRouting, Class<T> entityClass, ObjectMapper objectMapper) {
        this(schema, clientRouting, entityClass, objectMapper, ValkeyObservationInvoker.noop(), ValkeyUpdateMetricsRecorder.NOOP);
    }

    protected BaseValkeyRepository(
            IndexSchema schema,
            ValkeyClientRouting clientRouting,
            Class<T> entityClass,
            ObjectMapper objectMapper,
            ValkeyObservationInvoker observationInvoker) {
        this(schema, clientRouting, entityClass, objectMapper, observationInvoker, ValkeyUpdateMetricsRecorder.NOOP);
    }

    protected BaseValkeyRepository(
            IndexSchema schema,
            ValkeyClientRouting clientRouting,
            Class<T> entityClass,
            ObjectMapper objectMapper,
            ValkeyObservationInvoker observationInvoker,
            ValkeyUpdateMetricsRecorder updateMetricsRecorder) {
        this.schema = schema;
        this.clientRouting = clientRouting;
        this.entityClass = entityClass;
        this.objectMapper = objectMapper == null ? new ObjectMapper().findAndRegisterModules() : objectMapper;
        this.observationInvoker = observationInvoker == null ? ValkeyObservationInvoker.noop() : observationInvoker;
        this.updateMetricsRecorder = updateMetricsRecorder == null ? ValkeyUpdateMetricsRecorder.NOOP : updateMetricsRecorder;
        this.entityOperations = ValkeyEntityOperationsFactory.create(schema.storageType(), schema);
        this.resultMapper = new ValkeyResultMapper<>(schema, entityClass, this.objectMapper, entityOperations);
    }

    public String checkAndCreateIndex() {
        IndexDiff diff = inspectIndexDiff();
        IndexMigrationPlan plan = diff.plan();
        if (plan.isEmpty()) {
            return "OK";
        }
        if (plan.requiresRecreate()) {
            dropIndex(false);
        }
        return createIndex();
    }

    public void save(String id, T entity) {
        String key = buildKey(id);
        try {
            entityOperations.save(requireRouting(), observationInvoker, schema.indexName(), key, entity, objectMapper);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new ValkeyQueryExecutionException(ValkeyErrorCode.QUERY_SAVE_FAILED, "保存实体时线程被中断: " + key, exception);
        } catch (Exception exception) {
            rethrowKnownException(exception);
            throw new ValkeyQueryExecutionException(ValkeyErrorCode.QUERY_SAVE_FAILED, "保存实体失败: " + key, exception);
        }
    }

    @Override
    public BulkWriteResult saveAll(List<BulkSaveItem<T>> items, BulkWriteOptions options) {
        BulkWriteOptions resolved = options == null ? BulkWriteOptions.defaults() : options;
        if (items == null || items.isEmpty()) {
            return new BulkWriteResult(0, 0, 0, false, List.of());
        }
        List<BulkWriteItemResult> results = resolved.collectItemResults() ? new ArrayList<>() : List.of();
        int succeeded = 0;
        int failed = 0;
        for (int index = 0; index < items.size(); index += resolved.batchSize()) {
            List<BulkSaveItem<T>> batch = items.subList(index, Math.min(items.size(), index + resolved.batchSize()));
            BatchOutcome batchOutcome = saveBatch(batch, resolved);
            succeeded += batchOutcome.succeeded();
            failed += batchOutcome.failed();
            if (resolved.collectItemResults()) {
                results.addAll(batchOutcome.results());
            }
            if (batchOutcome.stoppedEarly()) {
                return new BulkWriteResult(items.size(), succeeded, failed, succeeded > 0 && failed > 0, results);
            }
        }
        return new BulkWriteResult(items.size(), succeeded, failed, succeeded > 0 && failed > 0, results);
    }

    @Override
    public BulkWriteResult deleteAll(List<String> ids, BulkWriteOptions options) {
        BulkWriteOptions resolved = options == null ? BulkWriteOptions.defaults() : options;
        if (ids == null || ids.isEmpty()) {
            return new BulkWriteResult(0, 0, 0, false, List.of());
        }
        List<BulkWriteItemResult> results = resolved.collectItemResults() ? new ArrayList<>() : List.of();
        int succeeded = 0;
        int failed = 0;
        for (int index = 0; index < ids.size(); index += resolved.batchSize()) {
            List<String> batch = ids.subList(index, Math.min(ids.size(), index + resolved.batchSize()));
            BatchOutcome batchOutcome = deleteBatch(batch, resolved);
            succeeded += batchOutcome.succeeded();
            failed += batchOutcome.failed();
            if (resolved.collectItemResults()) {
                results.addAll(batchOutcome.results());
            }
            if (batchOutcome.stoppedEarly()) {
                return new BulkWriteResult(ids.size(), succeeded, failed, succeeded > 0 && failed > 0, results);
            }
        }
        return new BulkWriteResult(ids.size(), succeeded, failed, succeeded > 0 && failed > 0, results);
    }

    @Override
    public BulkWriteResult updateAll(List<BulkUpdateItem> items, BulkWriteOptions options) {
        BulkWriteOptions resolved = options == null ? BulkWriteOptions.defaults() : options;
        if (items == null || items.isEmpty()) {
            return new BulkWriteResult(0, 0, 0, false, List.of());
        }
        List<BulkWriteItemResult> results = resolved.collectItemResults() ? new ArrayList<>() : List.of();
        int succeeded = 0;
        int failed = 0;
        for (int index = 0; index < items.size(); index += resolved.batchSize()) {
            List<BulkUpdateItem> batch = items.subList(index, Math.min(items.size(), index + resolved.batchSize()));
            BatchOutcome batchOutcome = updateBatch(batch, resolved);
            succeeded += batchOutcome.succeeded();
            failed += batchOutcome.failed();
            if (resolved.collectItemResults()) {
                results.addAll(batchOutcome.results());
            }
            if (batchOutcome.stoppedEarly()) {
                return new BulkWriteResult(items.size(), succeeded, failed, succeeded > 0 && failed > 0, results);
            }
        }
        return new BulkWriteResult(items.size(), succeeded, failed, succeeded > 0 && failed > 0, results);
    }

    @Override
    public long updateById(Object id, List<UpdateOperation> operations, SearchPredicate predicate) {
        String normalizedId = normalizeId(id);
        if (operations == null || operations.isEmpty()) {
            throw new IllegalArgumentException("Update operations cannot be empty");
        }
        String key = buildKey(normalizedId);
        try {
            String updateKind = summarizeUpdateKind(operations);
            if (predicate == null && !keyExists(key)) {
                updateMetricsRecorder.recordPartialUpdate(schema.indexName(), "not_found", updateKind);
                return 0L;
            }
            long updated = entityOperations.update(
                    requireRouting(),
                    observationInvoker,
                    schema.indexName(),
                    key,
                    operations,
                    predicate,
                    objectMapper
            );
            updateMetricsRecorder.recordPartialUpdate(schema.indexName(), updated > 0 ? "updated" : "condition_not_matched", updateKind);
            return updated;
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            updateMetricsRecorder.recordPartialUpdateFailure(schema.indexName(), ValkeyErrorCode.QUERY_UPDATE_FAILED.code(), ValkeyErrorCode.QUERY_UPDATE_FAILED.category(), summarizeUpdateKind(operations));
            throw new ValkeyQueryExecutionException(ValkeyErrorCode.QUERY_UPDATE_FAILED, "局部更新时线程被中断: " + key, exception);
        } catch (Exception exception) {
            if (exception instanceof ValkeyQueryException queryException) {
                updateMetricsRecorder.recordPartialUpdateFailure(schema.indexName(), queryException.errorCodeValue(), queryException.errorCategory(), summarizeUpdateKind(operations));
            } else {
                updateMetricsRecorder.recordPartialUpdateFailure(schema.indexName(), ValkeyErrorCode.QUERY_UPDATE_FAILED.code(), ValkeyErrorCode.QUERY_UPDATE_FAILED.category(), summarizeUpdateKind(operations));
            }
            rethrowKnownException(exception);
            throw new ValkeyQueryExecutionException(ValkeyErrorCode.QUERY_UPDATE_FAILED, "局部更新失败: " + key, exception);
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
        try {
            String[] command = buildCountCommand(condition);
            if (!isClusterMode()) {
                return countFromResponse(executeReadCommand(command));
            }
            long total = 0L;
            for (Object response : executeReadAllCommand(command)) {
                total += countFromResponse(response);
            }
            return total;
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new ValkeyQueryExecutionException(ValkeyErrorCode.QUERY_COUNT_FAILED, "执行检索时线程被中断: " + schema.indexName(), exception);
        } catch (Exception exception) {
            rethrowKnownException(exception);
            throw new ValkeyQueryExecutionException(ValkeyErrorCode.QUERY_COUNT_FAILED, "执行 count 查询失败: " + schema.indexName(), exception);
        }
    }

    @Override
    public AggregateResult aggregate(SearchCondition condition, AggregateRequest request) {
        AggregateRequest resolved = request == null ? new AggregateRequest(List.of(), List.of(), null, true, 0, 1_000) : request;
        try {
            if (isClusterMode()) {
                return executeClusterAggregate(condition, resolved);
            }
            return mapAggregateResponse(asObjectArray(executeReadCommand(buildAggregateCommand(condition, resolved))));
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new ValkeyQueryExecutionException(ValkeyErrorCode.QUERY_AGGREGATE_FAILED, "执行聚合时线程被中断: " + schema.indexName(), exception);
        } catch (Exception exception) {
            rethrowKnownException(exception);
            throw new ValkeyQueryExecutionException(ValkeyErrorCode.QUERY_AGGREGATE_FAILED, "执行聚合失败: " + schema.indexName(), exception);
        }
    }

    private AggregateResult executeClusterAggregate(SearchCondition condition, AggregateRequest request) throws Exception {
        validateClusterAggregateRequest(request);
        AggregateRequest shardRequest = toClusterShardAggregateRequest(request);
        List<Object> shardResponses = executeReadAllCommand(buildAggregateCommand(condition, shardRequest));
        return mergeClusterAggregateResponses(request, shardResponses);
    }

    public String getIndexName() {
        return schema.indexName();
    }

    public IndexSchema getSchema() {
        return schema;
    }

    public IndexDiff inspectIndexDiff() {
        try {
            boolean existsEverywhere = indexExists(schema.indexName());
            if (!existsEverywhere) {
                if (isClusterMode() && indexExistsOnAnyNode(schema.indexName())) {
                    return IndexDiff.of(schema.indexName(), new IndexDiffItem(
                            IndexDiffType.INDEX_PARTIALLY_MISSING,
                            schema.indexName(),
                            "present-on-all-shards",
                            "missing-on-some-shards"
                    ));
                }
                return IndexDiff.of(schema.indexName(), new IndexDiffItem(
                        IndexDiffType.INDEX_MISSING,
                        schema.indexName(),
                        "present",
                        "missing"
                ));
            }
            Object response = executeIndexCommand(new String[]{"FT.INFO", schema.indexName()});
            ExistingIndexInfo indexInfo = ExistingIndexInfo.fromResponse(response);
            return indexInfo.diff(schema, entityOperations.dataType());
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new ValkeyIndexException(ValkeyErrorCode.INDEX_READ_FAILED, "读取索引信息时线程被中断: " + schema.indexName(), exception);
        } catch (Exception exception) {
            rethrowKnownException(exception);
            throw new ValkeyIndexException(ValkeyErrorCode.INDEX_READ_FAILED, "读取索引信息失败: " + schema.indexName(), exception);
        }
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

    public String createIndex() {
        try {
            return String.valueOf(executeIndexCommand(buildCreateCommand()));
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new ValkeyIndexException(ValkeyErrorCode.INDEX_CREATE_FAILED, "创建索引时线程被中断: " + schema.indexName(), exception);
        } catch (Exception exception) {
            rethrowKnownException(exception);
            throw new ValkeyIndexException(ValkeyErrorCode.INDEX_CREATE_FAILED, "创建索引失败: " + schema.indexName(), exception);
        }
    }

    public String dropIndex(boolean deleteDocuments) {
        String[] command = buildDropIndexCommand(deleteDocuments);
        try {
            return String.valueOf(executeIndexCommand(command));
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new ValkeyIndexException(ValkeyErrorCode.INDEX_RECREATE_FAILED, "删除索引时线程被中断: " + schema.indexName(), exception);
        } catch (Exception exception) {
            if (deleteDocuments && supportsLegacyDropWithoutDeleteFlag(exception)) {
                try {
                    return String.valueOf(executeIndexCommand(buildDropIndexCommand(false)));
                } catch (InterruptedException interruptedException) {
                    Thread.currentThread().interrupt();
                    throw new ValkeyIndexException(ValkeyErrorCode.INDEX_RECREATE_FAILED, "删除索引时线程被中断: " + schema.indexName(), interruptedException);
                } catch (Exception retryException) {
                    rethrowKnownException(retryException);
                    throw new ValkeyIndexException(ValkeyErrorCode.INDEX_RECREATE_FAILED, "删除索引失败: " + schema.indexName(), retryException);
                }
            }
            rethrowKnownException(exception);
            throw new ValkeyIndexException(ValkeyErrorCode.INDEX_RECREATE_FAILED, "删除索引失败: " + schema.indexName(), exception);
        }
    }

    protected String buildKey(String id) {
        return schema.prefix() + id;
    }

    protected T convertStoredFields(Map<String, ?> fields) {
        try {
            return entityOperations.readEntity(objectMapper, entityClass, fields);
        } catch (Exception exception) {
            throw new ValkeyResultMappingException(ValkeyErrorCode.RESULT_MAPPING_ERROR, "结果映射失败: " + entityClass.getName(), exception);
        }
    }

    protected Map<String, String> toStringMap(T entity) {
        return entityOperations.toStorageFields(entity, objectMapper);
    }

    protected SearchResult<T> mapSearchResponse(Object[] response) {
        return resultMapper.mapSearchResponse(response);
    }

    protected AggregateResult mapAggregateResponse(Object[] response) {
        if (response.length == 0) {
            return new AggregateResult(0L, List.of());
        }
        if (isAggregateRowsOnlyResponse(response)) {
            List<AggregateRow> rows = new ArrayList<>(response.length);
            for (Object item : response) {
                rows.add(mapAggregateRow(item));
            }
            return new AggregateResult(rows.size(), rows);
        }
        long total = toLong(response[0]);
        List<AggregateRow> rows = new ArrayList<>(Math.max(0, response.length - 1));
        for (int i = 1; i < response.length; i++) {
            rows.add(mapAggregateRow(response[i]));
        }
        return new AggregateResult(total, rows);
    }

    protected ValkeyClientRouting requireRouting() {
        if (clientRouting == null) {
            throw new ValkeyConfigurationException(ValkeyErrorCode.CONFIGURATION_ERROR, "Valkey 客户端未注入，无法执行真实 Valkey I/O");
        }
        return clientRouting;
    }

    protected Object executeWriteCommand(String[] command) throws Exception {
        return observationInvoker.execute(
                commandName(command),
                schema.indexName(),
                () -> String.join(" ", command),
                "write",
                () -> requireRouting().executeWrite(command)
        );
    }

    protected Object executeReadCommand(String[] command) throws Exception {
        return observationInvoker.execute(
                commandName(command),
                schema.indexName(),
                () -> String.join(" ", command),
                "read",
                () -> requireRouting().executeRead(command)
        );
    }

    protected Object executeIndexCommand(String[] command) throws Exception {
        return observationInvoker.execute(
                commandName(command),
                schema.indexName(),
                () -> String.join(" ", command),
                "index",
                () -> requireRouting().executeIndex(command)
        );
    }

    protected List<Object> executeReadAllCommand(String[] command) throws Exception {
        return observationInvoker.execute(
                commandName(command),
                schema.indexName(),
                () -> String.join(" ", command),
                "read_all",
                () -> requireRouting().executeReadAll(command)
        );
    }

    protected List<Object> executeIndexAllCommand(String[] command) throws Exception {
        return observationInvoker.execute(
                commandName(command),
                schema.indexName(),
                () -> String.join(" ", command),
                "index_all",
                () -> requireRouting().executeIndexAll(command)
        );
    }

    private String summarizeUpdateKind(List<UpdateOperation> operations) {
        boolean hasSet = false;
        boolean hasIncrement = false;
        boolean hasDecrement = false;
        for (UpdateOperation operation : operations) {
            if (operation == null || operation.kind() == null) {
                continue;
            }
            switch (operation.kind()) {
                case SET -> hasSet = true;
                case INCREMENT -> hasIncrement = true;
                case DECREMENT -> hasDecrement = true;
            }
        }
        int activeKinds = (hasSet ? 1 : 0) + (hasIncrement ? 1 : 0) + (hasDecrement ? 1 : 0);
        if (activeKinds == 0) {
            return "unknown";
        }
        if (activeKinds > 1) {
            return "mixed";
        }
        if (hasSet) {
            return "set";
        }
        if (hasIncrement) {
            return "increment";
        }
        return "decrement";
    }

    protected boolean isClusterMode() {
        return requireRouting().isClusterMode();
    }

    protected boolean keyExists(String key) throws Exception {
        Object response = executeReadCommand(new String[]{"EXISTS", key});
        return toLong(response) > 0;
    }

    protected long deleteByIdInternal(Object id) throws Exception {
        String key = buildKey(normalizeId(id));
        Object response = executeWriteCommand(new String[]{"DEL", key});
        return toLong(response);
    }

    protected String[] buildCreateCommand() {
        List<String> command = new ArrayList<>();
        command.add("FT.CREATE");
        command.add(schema.indexName());
        command.add("ON");
        command.add(entityOperations.dataType().name());
        if (!schema.prefixes().isEmpty()) {
            command.add("PREFIX");
            command.add(Integer.toString(schema.prefixes().size()));
            command.addAll(schema.prefixes());
        }
        command.add("SCHEMA");
        for (SchemaField field : schema.fields()) {
            appendFieldDefinition(command, field);
        }
        return command.toArray(String[]::new);
    }

    protected String[] buildSearchCommand(SearchCondition condition, int offset, int limit) {
        List<String> command = new ArrayList<>();
        command.add("FT.SEARCH");
        command.add(schema.indexName());
        command.add(buildQuery(condition));
        if (entityOperations.dataType() == glide.api.models.commands.FT.FTCreateOptions.DataType.JSON) {
            appendJsonReturn(command, condition);
        } else if (condition != null && condition.hasProjection()) {
            command.add("RETURN");
            command.add(Integer.toString(condition.selectedFields().size()));
            command.addAll(condition.selectedFields());
        }
        if (condition != null && condition.hasSort()) {
            command.add("SORTBY");
            command.add(condition.sortField());
            command.add(condition.sortAscending() ? "ASC" : "DESC");
        }
        command.add("LIMIT");
        command.add(Integer.toString(Math.max(0, offset)));
        command.add(Integer.toString(Math.max(0, limit)));
        command.add("DIALECT");
        command.add("2");
        return command.toArray(String[]::new);
    }

    protected GlideString[] buildVectorSearchCommand(SearchCondition condition, int offset, int limit) {
        VectorQuery vectorQuery = condition.vectorQuery();
        List<GlideString> command = new ArrayList<>();
        command.add(GlideString.of("FT.SEARCH"));
        command.add(GlideString.of(schema.indexName()));
        command.add(GlideString.of(buildVectorQuery(condition)));
        appendReturn(command, condition);
        command.add(GlideString.of("PARAMS"));
        command.add(GlideString.of("2"));
        command.add(GlideString.of("vector"));
        command.add(GlideString.of(encodeFloat32(vectorQuery.vector())));
        command.add(GlideString.of("LIMIT"));
        command.add(GlideString.of(Integer.toString(Math.max(0, offset))));
        command.add(GlideString.of(Integer.toString(Math.max(0, limit))));
        command.add(GlideString.of("DIALECT"));
        command.add(GlideString.of("2"));
        return command.toArray(GlideString[]::new);
    }

    protected String[] buildAggregateCommand(SearchCondition condition, AggregateRequest request) {
        List<String> command = new ArrayList<>();
        command.add("FT.AGGREGATE");
        command.add(schema.indexName());
        command.add(buildQuery(condition));
        if (request != null) {
            List<String> loadFields = collectAggregateLoadFields(request);
            if (!loadFields.isEmpty()) {
                command.add("LOAD");
                command.add(Integer.toString(loadFields.size()));
                for (String fieldName : loadFields) {
                    command.add("@" + fieldName);
                }
            }
        }
        if (request != null && request.hasGroupBy()) {
            command.add("GROUPBY");
            command.add(Integer.toString(request.groupByFields().size()));
            for (String fieldName : request.groupByFields()) {
                command.add("@" + fieldName);
            }
        }
        if (request != null && request.hasReducers()) {
            for (AggregateReducer reducer : request.reducers()) {
                appendReducer(command, reducer);
            }
        }
        if (request != null && request.hasApplies()) {
            for (AggregateApply apply : request.applies()) {
                command.add("APPLY");
                command.add(apply.expression());
                command.add("AS");
                command.add(apply.alias());
            }
        }
        if (request != null && request.hasFilters()) {
            for (String filter : request.filters()) {
                command.add("FILTER");
                command.add(filter);
            }
        }
        if (request != null && request.hasSort()) {
            command.add("SORTBY");
            command.add("2");
            command.add("@" + request.sortField());
            command.add(request.sortAscending() ? "ASC" : "DESC");
        }
        if (request != null) {
            command.add("LIMIT");
            command.add(Integer.toString(request.offset()));
            command.add(Integer.toString(request.limit()));
        }
        command.add("DIALECT");
        command.add("2");
        return command.toArray(String[]::new);
    }

    private List<String> collectAggregateLoadFields(AggregateRequest request) {
        java.util.LinkedHashSet<String> fields = new java.util.LinkedHashSet<>();
        if (request.hasGroupBy()) {
            fields.addAll(request.groupByFields());
        }
        if (request.hasReducers()) {
            for (AggregateReducer reducer : request.reducers()) {
                if (reducer.kind() == AggregateReducerKind.COUNT) {
                    continue;
                }
                if (reducer.fieldName() != null && !reducer.fieldName().isBlank()) {
                    fields.add(reducer.fieldName());
                }
            }
        }
        return List.copyOf(fields);
    }

    private void validateClusterAggregateRequest(AggregateRequest request) {
        if (request.hasApplies() || request.hasFilters()) {
            throw new ValkeyQueryExecutionException(
                    ValkeyErrorCode.QUERY_AGGREGATE_FAILED,
                    "cluster 聚合当前仅支持 GROUPBY + COUNT/SUM/AVG/MIN/MAX + SORTBY + LIMIT，不支持 APPLY/FILTER: " + schema.indexName()
            );
        }
        for (AggregateReducer reducer : request.reducers()) {
            if (reducer.kind() == AggregateReducerKind.COUNT_DISTINCT) {
                throw new ValkeyQueryExecutionException(
                        ValkeyErrorCode.QUERY_AGGREGATE_FAILED,
                        "cluster 聚合当前不支持 COUNT_DISTINCT: " + schema.indexName()
                );
            }
        }
    }

    private AggregateRequest toClusterShardAggregateRequest(AggregateRequest request) {
        List<AggregateReducer> shardReducers = new ArrayList<>();
        for (AggregateReducer reducer : request.reducers()) {
            if (reducer.kind() == AggregateReducerKind.AVG) {
                shardReducers.add(AggregateReducer.sum(reducer.fieldName(), averageSumAlias(reducer.alias())));
                shardReducers.add(AggregateReducer.count(averageCountAlias(reducer.alias())));
                continue;
            }
            shardReducers.add(reducer);
        }
        String sortField = request.sortField();
        if (sortField != null) {
            for (AggregateReducer reducer : request.reducers()) {
                if (reducer.kind() == AggregateReducerKind.AVG && reducer.alias().equals(sortField)) {
                    sortField = averageSumAlias(sortField);
                    break;
                }
            }
        }
        return new AggregateRequest(request.groupByFields(), shardReducers, sortField, request.sortAscending(), 0, clusterShardLimit(request.offset(), request.limit()));
    }

    private AggregateResult mergeClusterAggregateResponses(AggregateRequest request, List<Object> shardResponses) {
        Map<GroupKey, Map<String, Object>> merged = new LinkedHashMap<>();
        for (Object shardResponse : shardResponses) {
            AggregateResult partial = mapAggregateResponse(asObjectArray(shardResponse));
            for (AggregateRow row : partial.rows()) {
                GroupKey key = GroupKey.of(request.groupByFields(), row);
                Map<String, Object> target = merged.computeIfAbsent(key, ignored -> new LinkedHashMap<>(key.toValueMap()));
                mergeAggregateRow(target, row.values(), request);
            }
        }
        finalizeAverageReducers(merged.values(), request);
        List<AggregateRow> rows = merged.values().stream().map(AggregateRow::new).toList();
        List<AggregateRow> sorted = sortAggregateRows(rows, request);
        int start = Math.min(request.offset(), sorted.size());
        int end = Math.min(sorted.size(), start + request.limit());
        return new AggregateResult(sorted.size(), sorted.subList(start, end));
    }

    private void mergeAggregateRow(Map<String, Object> target, Map<String, Object> incoming, AggregateRequest request) {
        for (AggregateReducer reducer : request.reducers()) {
            switch (reducer.kind()) {
                case COUNT -> mergeLong(target, incoming, reducer.alias(), Long::sum);
                case COUNT_DISTINCT -> throw new ValkeyQueryExecutionException(
                        ValkeyErrorCode.QUERY_AGGREGATE_FAILED,
                        "cluster 聚合当前不支持 COUNT_DISTINCT: " + schema.indexName()
                );
                case SUM -> mergeDouble(target, incoming, reducer.alias(), Double::sum);
                case MIN -> mergeDouble(target, incoming, reducer.alias(), Math::min);
                case MAX -> mergeDouble(target, incoming, reducer.alias(), Math::max);
                case AVG -> {
                    mergeDouble(target, incoming, averageSumAlias(reducer.alias()), Double::sum);
                    mergeLong(target, incoming, averageCountAlias(reducer.alias()), Long::sum);
                }
            }
        }
    }

    private void finalizeAverageReducers(Iterable<Map<String, Object>> rows, AggregateRequest request) {
        for (Map<String, Object> row : rows) {
            for (AggregateReducer reducer : request.reducers()) {
                if (reducer.kind() != AggregateReducerKind.AVG) {
                    continue;
                }
                double sum = asDouble(row.get(averageSumAlias(reducer.alias())));
                long count = asLong(row.get(averageCountAlias(reducer.alias())));
                row.put(reducer.alias(), count == 0L ? 0d : sum / count);
            }
        }
    }

    private List<AggregateRow> sortAggregateRows(List<AggregateRow> rows, AggregateRequest request) {
        if (!request.hasSort()) {
            return new ArrayList<>(rows);
        }
        List<AggregateRow> sorted = new ArrayList<>(rows);
        Comparator<AggregateRow> comparator = Comparator.comparing(
                row -> aggregateComparable(row.get(request.sortField())),
                Comparator.nullsLast(Comparator.naturalOrder())
        );
        sorted.sort(request.sortAscending() ? comparator : comparator.reversed());
        return sorted;
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private Comparable aggregateComparable(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Comparable comparable) {
            return comparable;
        }
        return String.valueOf(value);
    }

    private void mergeLong(Map<String, Object> target, Map<String, Object> incoming, String field, java.util.function.LongBinaryOperator operator) {
        if (!incoming.containsKey(field)) {
            return;
        }
        long current = asLong(target.get(field));
        long next = asLong(incoming.get(field));
        target.put(field, operator.applyAsLong(current, next));
    }

    private void mergeDouble(Map<String, Object> target, Map<String, Object> incoming, String field, java.util.function.DoubleBinaryOperator operator) {
        if (!incoming.containsKey(field)) {
            return;
        }
        if (!target.containsKey(field)) {
            target.put(field, asDouble(incoming.get(field)));
            return;
        }
        double current = asDouble(target.get(field));
        double next = asDouble(incoming.get(field));
        target.put(field, operator.applyAsDouble(current, next));
    }

    private long asLong(Object value) {
        if (value == null) {
            return 0L;
        }
        if (value instanceof Number number) {
            return number.longValue();
        }
        return Long.parseLong(String.valueOf(value));
    }

    private double asDouble(Object value) {
        if (value == null) {
            return 0d;
        }
        if (value instanceof Number number) {
            return number.doubleValue();
        }
        return Double.parseDouble(String.valueOf(value));
    }

    private String averageSumAlias(String alias) {
        return "__avg_sum_" + alias;
    }

    private String averageCountAlias(String alias) {
        return "__avg_count_" + alias;
    }

    private SearchResult<T> executeSearch(SearchCondition condition, int offset, int limit) {
        try {
            if (condition != null && condition.hasVectorQuery()) {
                if (isClusterMode()) {
                    throw new ValkeyQueryExecutionException(
                            ValkeyErrorCode.QUERY_SEARCH_FAILED,
                            "cluster 模式当前不支持向量 KNN 查询: " + schema.indexName()
                    );
                }
                GlideString[] command = buildVectorSearchCommand(condition, offset, limit);
                Object response = observationInvoker.execute(
                        "FT.SEARCH",
                        schema.indexName(),
                        () -> buildVectorQuery(condition),
                        "read",
                        () -> requireRouting().executeRead(command)
                );
                return mapSearchResponse(asObjectArray(response));
            }
            if (!isClusterMode()) {
                String[] command = buildSearchCommand(condition, offset, limit);
                return mapSearchResponse(asObjectArray(executeReadCommand(command)));
            }
            int mergeWindow = clusterShardLimit(offset, limit);
            String[] command = buildSearchCommand(condition, 0, mergeWindow);
            return mergeClusterSearchResponses(condition, offset, limit, mergeWindow, executeReadAllCommand(command));
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new ValkeyQueryExecutionException(ValkeyErrorCode.QUERY_SEARCH_FAILED, "执行检索时线程被中断: " + schema.indexName(), exception);
        } catch (Exception exception) {
            rethrowKnownException(exception);
            throw new ValkeyQueryExecutionException(ValkeyErrorCode.QUERY_SEARCH_FAILED, "执行检索与结果映射失败: " + schema.indexName(), exception);
        }
    }

    private SearchResult<T> mergeClusterSearchResponses(SearchCondition condition, int offset, int limit, int mergeWindow, List<Object> shardResponses) {
        if (mergeWindow <= 0) {
            long total = 0L;
            for (Object shardResponse : shardResponses) {
                total += mapSearchResponse(asObjectArray(shardResponse)).total();
            }
            return new SearchResult<>(total, List.of());
        }
        if (condition != null && condition.hasSort()) {
            return mergeSortedClusterSearchResponses(condition, offset, limit, mergeWindow, shardResponses);
        }
        return mergeUnsortedClusterSearchResponses(offset, limit, mergeWindow, shardResponses);
    }

    private SearchResult<T> mergeSortedClusterSearchResponses(SearchCondition condition, int offset, int limit, int mergeWindow, List<Object> shardResponses) {
        long total = 0L;
        Comparator<T> comparator = buildSortComparator(condition);
        PriorityQueue<T> topWindow = new PriorityQueue<>(Math.max(1, mergeWindow), comparator.reversed());
        for (Object shardResponse : shardResponses) {
            SearchResult<T> partial = mapSearchResponse(asObjectArray(shardResponse));
            total += partial.total();
            for (T record : partial.records()) {
                if (topWindow.size() < mergeWindow) {
                    topWindow.offer(record);
                    continue;
                }
                T worst = topWindow.peek();
                if (worst != null && comparator.compare(record, worst) < 0) {
                    topWindow.poll();
                    topWindow.offer(record);
                }
            }
        }
        List<T> mergedRecords = new ArrayList<>(topWindow);
        mergedRecords.sort(comparator);
        int start = Math.min(Math.max(0, offset), mergedRecords.size());
        int end = Math.min(mergedRecords.size(), start + Math.max(0, limit));
        return new SearchResult<>(total, new ArrayList<>(mergedRecords.subList(start, end)));
    }

    private SearchResult<T> mergeUnsortedClusterSearchResponses(int offset, int limit, int mergeWindow, List<Object> shardResponses) {
        long total = 0L;
        List<T> mergedRecords = new ArrayList<>(Math.min(mergeWindow, 128));
        for (Object shardResponse : shardResponses) {
            SearchResult<T> partial = mapSearchResponse(asObjectArray(shardResponse));
            total += partial.total();
            if (mergedRecords.size() >= mergeWindow) {
                continue;
            }
            List<T> records = partial.records();
            int remaining = mergeWindow - mergedRecords.size();
            int copySize = Math.min(remaining, records.size());
            mergedRecords.addAll(records.subList(0, copySize));
        }
        int start = Math.min(Math.max(0, offset), mergedRecords.size());
        int end = Math.min(mergedRecords.size(), start + Math.max(0, limit));
        return new SearchResult<>(total, new ArrayList<>(mergedRecords.subList(start, end)));
    }

    private Comparator<T> buildSortComparator(SearchCondition condition) {
        Comparator<T> comparator = Comparator.comparing(
                entity -> extractComparableValue(entity, resolveSortPath(condition.sortField())),
                Comparator.nullsLast(Comparator.naturalOrder())
        );
        return condition.sortAscending() ? comparator : comparator.reversed();
    }

    @SuppressWarnings({"rawtypes", "unchecked"})
    private Comparable extractComparableValue(T entity, String path) {
        Object value = readPathValue(entity, path);
        if (value == null) {
            return null;
        }
        if (value instanceof Comparable comparable) {
            return comparable;
        }
        return String.valueOf(value);
    }

    private Object readPathValue(Object source, String path) {
        if (source == null || path == null || path.isBlank()) {
            return null;
        }
        Object current = source;
        for (String part : path.split("\\.")) {
            if (current == null || part.isBlank()) {
                return null;
            }
            current = readFieldValue(current, part);
        }
        return current;
    }

    private Object readFieldValue(Object source, String fieldName) {
        Class<?> type = source.getClass();
        while (type != null && type != Object.class) {
            try {
                Field field = type.getDeclaredField(fieldName);
                field.setAccessible(true);
                return field.get(source);
            } catch (NoSuchFieldException ignored) {
                type = type.getSuperclass();
            } catch (ReflectiveOperationException exception) {
                throw new ValkeyResultMappingException(ValkeyErrorCode.RESULT_SORT_FIELD_READ_FAILED, "读取排序字段失败: " + source.getClass().getName() + "." + fieldName, exception);
            }
        }
        return null;
    }

    private long countFromResponse(Object response) {
        Object[] values = asObjectArray(response);
        return values.length == 0 ? 0L : toLong(values[0]);
    }

    private String buildQuery(SearchCondition condition) {
        return condition == null || condition.build().isBlank() ? "*" : condition.build();
    }

    private String buildVectorQuery(SearchCondition condition) {
        String filter = buildQuery(condition);
        VectorQuery vectorQuery = condition.vectorQuery();
        return "(" + filter + ")=>[KNN " + vectorQuery.k() + " @" + vectorQuery.fieldName() + " $vector AS " + vectorQuery.scoreAlias() + "]";
    }

    private byte[] encodeFloat32(float[] values) {
        ByteBuffer buffer = ByteBuffer.allocate(values.length * Float.BYTES).order(ByteOrder.LITTLE_ENDIAN);
        for (float value : values) {
            buffer.putFloat(value);
        }
        return buffer.array();
    }

    private boolean indexExists(String indexName) {
        try {
            if (!isClusterMode()) {
                return responseContainsIndex(executeIndexCommand(new String[]{"FT._LIST"}), indexName);
            }
            List<Object> responses = executeIndexAllCommand(new String[]{"FT._LIST"});
            if (responses.isEmpty()) {
                return false;
            }
            for (Object response : responses) {
                if (!responseContainsIndex(response, indexName)) {
                    return false;
                }
            }
            return true;
        } catch (Exception exception) {
            return false;
        }
    }

    private boolean indexExistsOnAnyNode(String indexName) {
        try {
            for (Object response : executeIndexAllCommand(new String[]{"FT._LIST"})) {
                if (responseContainsIndex(response, indexName)) {
                    return true;
                }
            }
            return false;
        } catch (Exception exception) {
            return false;
        }
    }

    private boolean responseContainsIndex(Object response, String indexName) {
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
    }

    private void rethrowKnownException(Exception exception) {
        if (exception instanceof ValkeyConfigurationException configurationException) {
            throw configurationException;
        }
        if (exception instanceof ValkeyIndexException indexException) {
            throw indexException;
        }
        if (exception instanceof ValkeyResultMappingException mappingException) {
            throw mappingException;
        }
        if (exception instanceof ValkeyQueryExecutionException executionException) {
            throw executionException;
        }
    }

    private void appendFieldDefinition(List<String> command, SchemaField field) {
        if (entityOperations.dataType() == glide.api.models.commands.FT.FTCreateOptions.DataType.JSON) {
            command.add("$." + normalizeIndexedJsonPath(field));
            command.add("AS");
            command.add(field.fieldName());
        } else {
            command.add(field.fieldName());
        }

        if (field.type() == FieldType.VECTOR) {
            SchemaField.VectorOptions options = field.vectorOptions();
            if (options == null) {
                throw new IllegalStateException("Vector schema field is missing vector options: " + field.fieldName());
            }
            command.add("VECTOR");
            command.add("HNSW");
            command.add("10");
            command.add("TYPE");
            command.add("FLOAT32");
            command.add("DIM");
            command.add(Integer.toString(options.dimension()));
            command.add("DISTANCE_METRIC");
            command.add(options.distanceMetric().name());
            command.add("M");
            command.add(Integer.toString(options.m()));
            command.add("EF_CONSTRUCTION");
            command.add(Integer.toString(options.efConstruction()));
            return;
        }

        if (field.type() == FieldType.TEXT) {
            command.add("TEXT");
            if (field.noStem()) {
                command.add("NOSTEM");
            }
            if (field.sortable()) {
                command.add("SORTABLE");
            }
            command.add("WITHSUFFIXTRIE");
            return;
        }

        if (field.type() == FieldType.TAG) {
            command.add("TAG");
            command.add("SEPARATOR");
            command.add(field.separator());
            if (field.sortable()) {
                command.add("SORTABLE");
            }
            return;
        }

        command.add("NUMERIC");
        if (field.sortable()) {
            command.add("SORTABLE");
        }
    }

    private String[] buildCountCommand(SearchCondition condition) {
        return new String[]{
                "FT.SEARCH",
                schema.indexName(),
                buildQuery(condition),
                "LIMIT",
                "0",
                "0",
                "DIALECT",
                "2"
        };
    }

    private AggregateRow mapAggregateRow(Object rawRow) {
        if (rawRow instanceof Map<?, ?> map) {
            Map<String, Object> values = new LinkedHashMap<>();
            for (Map.Entry<?, ?> entry : map.entrySet()) {
                values.put(normalizeAggregateKey(String.valueOf(entry.getKey())), normalizeAggregateValue(entry.getValue()));
            }
            return new AggregateRow(values);
        }
        Object[] values = asObjectArray(rawRow);
        Map<String, Object> row = new LinkedHashMap<>();
        for (int i = 0; i + 1 < values.length; i += 2) {
            row.put(normalizeAggregateKey(String.valueOf(values[i])), normalizeAggregateValue(values[i + 1]));
        }
        return new AggregateRow(row);
    }

    private String normalizeAggregateKey(String rawKey) {
        if (rawKey == null || rawKey.isBlank()) {
            return rawKey;
        }
        String normalized = rawKey.startsWith("@") ? rawKey.substring(1) : rawKey;
        for (SchemaField field : schema.fields()) {
            if (normalized.equals(field.fieldName())) {
                return field.fieldName();
            }
            if (normalized.equals(field.jsonPath())
                    || normalized.equals(field.jsonPath().replace("[*]", ""))
                    || normalized.equals("$." + field.jsonPath())
                    || normalized.equals("$." + field.jsonPath().replace("[*]", ""))) {
                return field.fieldName();
            }
        }
        return normalized;
    }

    private Object normalizeAggregateValue(Object value) {
        if (value == null || value instanceof Number) {
            return value;
        }
        String stringValue = String.valueOf(value);
        if (stringValue.matches("-?\\d+")) {
            try {
                return Long.parseLong(stringValue);
            } catch (NumberFormatException ignored) {
                return stringValue;
            }
        }
        if (stringValue.matches("-?\\d+\\.\\d+")) {
            try {
                return Double.parseDouble(stringValue);
            } catch (NumberFormatException ignored) {
                return stringValue;
            }
        }
        return stringValue;
    }

    private void appendReducer(List<String> command, AggregateReducer reducer) {
        command.add("REDUCE");
        command.add(reducer.kind().name());
        if (reducer.kind() == AggregateReducerKind.COUNT) {
            command.add("0");
        } else {
            command.add("1");
            command.add("@" + reducer.fieldName());
        }
        command.add("AS");
        command.add(reducer.alias());
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

    private boolean isAggregateRowsOnlyResponse(Object[] response) {
        Object first = response[0];
        if (first == null) {
            return true;
        }
        if (first instanceof Number) {
            return false;
        }
        if (first instanceof Map<?, ?>) {
            return true;
        }
        String value = String.valueOf(first);
        return !value.matches("-?\\d+");
    }

    private long toLong(Object value) {
        if (value instanceof Number number) {
            return number.longValue();
        }
        return Long.parseLong(String.valueOf(value));
    }

    private String normalizeId(Object id) {
        if (id == null) {
            throw new IllegalArgumentException("Id cannot be null");
        }
        String normalized = String.valueOf(id);
        if (normalized.isBlank()) {
            throw new IllegalArgumentException("Id cannot be blank");
        }
        return normalized;
    }

    private BulkWriteItemResult toBulkFailure(String operation, String id, Exception exception) {
        if (exception instanceof ValkeyQueryException queryException) {
            return BulkWriteItemResult.failure(operation, id, queryException.errorCodeValue(), queryException.getMessage());
        }
        return BulkWriteItemResult.failure(operation, id, ValkeyErrorCode.QUERY_EXECUTION_ERROR.code(), exception.getMessage());
    }

    private BatchOutcome saveBatch(List<BulkSaveItem<T>> batch, BulkWriteOptions options) {
        List<CommandBatch<BulkSaveItem<T>>> commandBatch = new ArrayList<>(batch.size());
        List<BulkWriteItemResult> results = options.collectItemResults() ? new ArrayList<>() : List.of();
        int succeeded = 0;
        int failed = 0;
        for (BulkSaveItem<T> item : batch) {
            try {
                commandBatch.add(new CommandBatch<>(item, entityOperations.buildSaveCommand(buildKey(item.id()), item.entity(), objectMapper)));
            } catch (Exception exception) {
                failed++;
                if (options.collectItemResults()) {
                    results.add(toBulkFailure("save", item.id(), exception));
                }
                if (options.mode() == BulkMode.ORDERED) {
                    return new BatchOutcome(succeeded, failed, results, true);
                }
            }
        }
        BatchOutcome execution = executeCommandBatch(
                commandBatch,
                options,
                "save",
                batchSummary("save", commandBatch.size()),
                item -> item.id()
        );
        if (!options.collectItemResults() && !execution.results().isEmpty()) {
            return new BatchOutcome(succeeded + execution.succeeded(), failed + execution.failed(), List.of(), execution.stoppedEarly());
        }
        if (options.collectItemResults()) {
            results.addAll(execution.results());
        }
        return new BatchOutcome(succeeded + execution.succeeded(), failed + execution.failed(), results, execution.stoppedEarly());
    }

    private BatchOutcome deleteBatch(List<String> ids, BulkWriteOptions options) {
        List<CommandBatch<String>> commandBatch = new ArrayList<>(ids.size());
        for (String id : ids) {
            commandBatch.add(new CommandBatch<>(id, new String[]{"DEL", buildKey(normalizeId(id))}));
        }
        return executeCommandBatch(
                commandBatch,
                options,
                "delete",
                batchSummary("delete", commandBatch.size()),
                id -> id
        );
    }

    private BatchOutcome updateBatch(List<BulkUpdateItem> batch, BulkWriteOptions options) {
        List<BulkWriteItemResult> results = options.collectItemResults() ? new ArrayList<>() : List.of();
        int succeeded = 0;
        int failed = 0;
        for (BulkUpdateItem item : batch) {
            try {
                long updated = updateById(item.id(), item.operations(), item.predicate());
                if (updated > 0L) {
                    succeeded++;
                    if (options.collectItemResults()) {
                        results.add(BulkWriteItemResult.success("update", item.id()));
                    }
                    continue;
                }
                failed++;
                if (options.collectItemResults()) {
                    results.add(BulkWriteItemResult.failure("update", item.id(), ValkeyErrorCode.QUERY_UPDATE_FAILED.code(), "No document updated"));
                }
                if (options.mode() == BulkMode.ORDERED) {
                    return new BatchOutcome(succeeded, failed, results, true);
                }
            } catch (Exception exception) {
                failed++;
                if (options.collectItemResults()) {
                    results.add(toBulkFailure("update", item.id(), exception));
                }
                if (options.mode() == BulkMode.ORDERED) {
                    return new BatchOutcome(succeeded, failed, results, true);
                }
            }
        }
        return new BatchOutcome(succeeded, failed, results, false);
    }

    private <I> BatchOutcome executeCommandBatch(
            List<CommandBatch<I>> commands,
            BulkWriteOptions options,
            String operation,
            String summary,
            java.util.function.Function<I, String> idExtractor) {
        if (commands.isEmpty()) {
            return new BatchOutcome(0, 0, List.of(), false);
        }
        List<String[]> payload = new ArrayList<>(commands.size());
        for (CommandBatch<I> commandBatch : commands) {
            payload.add(commandBatch.command());
        }
        List<BatchCommandResult> batchResults;
        try {
            batchResults = observationInvoker.execute(
                    "BULK.WRITE",
                    schema.indexName(),
                    () -> summary,
                    "write",
                    () -> requireRouting().executeWriteBatch(payload, options.mode())
            );
        } catch (Exception exception) {
            BatchCommandResult failure = BatchCommandResult.failure(exception);
            batchResults = List.of(failure);
        }
        List<BulkWriteItemResult> itemResults = options.collectItemResults() ? new ArrayList<>() : List.of();
        int succeeded = 0;
        int failed = 0;
        for (int index = 0; index < batchResults.size(); index++) {
            BatchCommandResult result = batchResults.get(index);
            String id = idExtractor.apply(commands.get(index).item());
            if (result.success()) {
                succeeded++;
                if (options.collectItemResults()) {
                    itemResults.add(BulkWriteItemResult.success(operation, id));
                }
                continue;
            }
            failed++;
            if (options.collectItemResults()) {
                itemResults.add(toBulkFailure(operation, id, result.error()));
            }
            if (options.mode() == BulkMode.ORDERED) {
                return new BatchOutcome(succeeded, failed, itemResults, true);
            }
        }
        return new BatchOutcome(succeeded, failed, itemResults, false);
    }

    private String batchSummary(String operation, int size) {
        return "bulk " + operation + " <count=" + size + ">";
    }

    private record BatchOutcome(
            int succeeded,
            int failed,
            List<BulkWriteItemResult> results,
            boolean stoppedEarly
    ) {
    }

    private int clusterShardLimit(int offset, int limit) {
        if (limit <= 0) {
            return 0;
        }
        long requested = (long) Math.max(0, offset) + limit;
        if (requested > MAX_CLUSTER_MERGE_WINDOW) {
            throw new ValkeyQueryExecutionException(
                    ValkeyErrorCode.QUERY_SEARCH_FAILED,
                    "Cluster merge search window exceeds supported limit: requested=" + requested + ", max=" + MAX_CLUSTER_MERGE_WINDOW
            );
        }
        return (int) requested;
    }

    private String resolveSortPath(String sortField) {
        for (SchemaField field : schema.fields()) {
            if (field.fieldName().equals(sortField)) {
                String jsonPath = field.jsonPath();
                String normalized = jsonPath.startsWith("$.") ? jsonPath.substring(2) : jsonPath;
                return normalized.replace("[*]", "");
            }
        }
        return sortField;
    }

    private String commandName(String[] command) {
        return command == null || command.length == 0 ? "UNKNOWN" : command[0];
    }

    private String normalizeIndexedJsonPath(SchemaField field) {
        if (field.type() == FieldType.TAG
                && entityOperations.dataType() == glide.api.models.commands.FT.FTCreateOptions.DataType.JSON
                && field.jsonPath().endsWith("[*]")) {
            return field.jsonPath().substring(0, field.jsonPath().length() - 3);
        }
        return field.jsonPath();
    }

    private String[] buildDropIndexCommand(boolean deleteDocuments) {
        List<String> command = new ArrayList<>();
        command.add("FT.DROPINDEX");
        command.add(schema.indexName());
        if (deleteDocuments) {
            command.add("DD");
        }
        return command.toArray(String[]::new);
    }

    private boolean supportsLegacyDropWithoutDeleteFlag(Exception exception) {
        Throwable current = exception;
        while (current != null) {
            String message = current.getMessage();
            if (message != null && message.contains("wrong number of arguments for 'FT.DROPINDEX'")) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private void appendJsonReturn(List<String> command, SearchCondition condition) {
        if (condition == null || !condition.hasProjection()) {
            command.add("RETURN");
            command.add("1");
            command.add("$");
            return;
        }
        List<String> projection = new ArrayList<>();
        for (String fieldName : condition.selectedFields()) {
            SchemaField field = resolveField(fieldName);
            projection.add("$." + normalizeIndexedJsonPath(field));
            projection.add("AS");
            projection.add(field.fieldName());
        }
        command.add("RETURN");
        command.add(Integer.toString(projection.size()));
        command.addAll(projection);
    }

    private void appendReturn(List<GlideString> command, SearchCondition condition) {
        if (entityOperations.dataType() == glide.api.models.commands.FT.FTCreateOptions.DataType.JSON) {
            if (condition == null || !condition.hasProjection()) {
                command.add(GlideString.of("RETURN"));
                command.add(GlideString.of("1"));
                command.add(GlideString.of("$"));
                return;
            }
            List<String> projection = new ArrayList<>();
            for (String fieldName : condition.selectedFields()) {
                SchemaField field = resolveField(fieldName);
                projection.add("$." + normalizeIndexedJsonPath(field));
                projection.add("AS");
                projection.add(field.fieldName());
            }
            command.add(GlideString.of("RETURN"));
            command.add(GlideString.of(Integer.toString(projection.size())));
            for (String token : projection) {
                command.add(GlideString.of(token));
            }
            return;
        }
        if (condition != null && condition.hasProjection()) {
            command.add(GlideString.of("RETURN"));
            command.add(GlideString.of(Integer.toString(condition.selectedFields().size())));
            for (String field : condition.selectedFields()) {
                command.add(GlideString.of(field));
            }
        }
    }

    private SchemaField resolveField(String fieldName) {
        for (SchemaField field : schema.fields()) {
            if (field.fieldName().equals(fieldName)) {
                return field;
            }
        }
        throw new IllegalArgumentException("Unknown schema field: " + fieldName);
    }
}
