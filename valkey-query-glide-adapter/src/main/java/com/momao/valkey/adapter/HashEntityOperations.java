package com.momao.valkey.adapter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.momao.valkey.adapter.observability.ValkeyObservationInvoker;
import com.momao.valkey.core.SearchPredicate;
import com.momao.valkey.core.NumericUpdateOperation;
import com.momao.valkey.core.UpdateOperation;
import com.momao.valkey.core.UpdateAssignment;
import com.momao.valkey.core.UpdateOperationKind;
import com.momao.valkey.core.metadata.IndexSchema;
import com.momao.valkey.core.metadata.SchemaField;
import com.momao.valkey.annotation.FieldType;
import glide.api.models.commands.FT.FTCreateOptions;
import glide.api.models.commands.FT.FTCreateOptions.FieldInfo;
import glide.api.models.commands.FT.FTCreateOptions.NumericField;
import glide.api.models.commands.FT.FTCreateOptions.TagField;
import glide.api.models.commands.FT.FTCreateOptions.TextField;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;

final class HashEntityOperations<T> implements ValkeyEntityOperations<T> {

    private static final Map<ObjectMapper, ObjectMapper> SNAKE_CASE_MAPPERS =
            java.util.Collections.synchronizedMap(new WeakHashMap<>());

    private final IndexSchema schema;

    private final ValkeyScriptExecutor scriptExecutor;

    HashEntityOperations(IndexSchema schema) {
        this(schema, ValkeyScriptExecutor.DEFAULT);
    }

    HashEntityOperations(IndexSchema schema, ValkeyScriptExecutor scriptExecutor) {
        this.schema = schema;
        this.scriptExecutor = scriptExecutor;
    }

    @Override
    public String[] buildSaveCommand(String key, T entity, ObjectMapper objectMapper) {
        Map<String, String> fields = toStorageFields(entity, objectMapper);
        String[] command = new String[2 + fields.size() * 2];
        command[0] = "HSET";
        command[1] = key;
        int index = 2;
        for (Map.Entry<String, String> entry : fields.entrySet()) {
            command[index++] = entry.getKey();
            command[index++] = entry.getValue();
        }
        return command;
    }

    @Override
    public void save(
            ValkeyClientRouting clientRouting,
            ValkeyObservationInvoker observationInvoker,
            String indexName,
            String key,
            T entity,
            ObjectMapper objectMapper) throws Exception {
        Map<String, String> fields = toStorageFields(entity, objectMapper);
        String[] command = buildSaveCommand(key, entity, objectMapper);
        observationInvoker.execute("HSET", indexName, () -> "HSET " + key + " <fields=" + fields.size() + ">", "write", () -> clientRouting.executeWrite(command));
    }

    @Override
    public long update(
            ValkeyClientRouting clientRouting,
            ValkeyObservationInvoker observationInvoker,
            String indexName,
            String key,
            List<UpdateOperation> operations,
            SearchPredicate predicate,
            ObjectMapper objectMapper) throws Exception {
        if (predicate == null && operations.size() == 1 && operations.get(0) instanceof NumericUpdateOperation numericOperation) {
            return executeNativeNumericUpdate(clientRouting, observationInvoker, indexName, key, numericOperation);
        }
        if (predicate != null || containsNumericOperation(operations)) {
            return executeScriptedUpdate(clientRouting, observationInvoker, indexName, key, operations, predicate, objectMapper);
        }
        List<UpdateAssignment> assignments = requireAssignments(operations);
        String[] command = new String[2 + assignments.size() * 2];
        command[0] = "HSET";
        command[1] = key;
        int index = 2;
        for (UpdateAssignment assignment : assignments) {
            command[index++] = assignment.fieldName();
            command[index++] = serializeValue(assignment.value(), objectMapper);
        }
        observationInvoker.execute("HSET", indexName, () -> "HSET " + key + " <fields=" + assignments.size() + ">", "write", () -> clientRouting.executeWrite(command));
        return 1L;
    }

    @Override
    public Map<String, String> toStorageFields(T entity, ObjectMapper objectMapper) {
        Map<String, Object> rawMap = snakeCaseMapper(objectMapper).convertValue(
                entity,
                objectMapper.getTypeFactory().constructMapType(LinkedHashMap.class, String.class, Object.class));
        Map<String, String> stringMap = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : rawMap.entrySet()) {
            if (entry.getValue() != null) {
                stringMap.put(entry.getKey(), String.valueOf(entry.getValue()));
            }
        }
        return stringMap;
    }

    @Override
    public T readEntity(ObjectMapper objectMapper, Class<T> entityClass, Map<String, ?> storedFields) {
        return snakeCaseMapper(objectMapper).convertValue(storedFields, entityClass);
    }

    @Override
    public FTCreateOptions.DataType dataType() {
        return FTCreateOptions.DataType.HASH;
    }

    @Override
    public FieldInfo toFieldInfo(SchemaField field) {
        return switch (field.type()) {
            case TEXT -> new FieldInfo(field.fieldName(), new TextField());
            case TAG -> new FieldInfo(field.fieldName(), buildTagField(field));
            case NUMERIC -> new FieldInfo(field.fieldName(), new NumericField());
            case VECTOR -> throw new UnsupportedOperationException("Vector fields are not supported for HASH storage");
        };
    }

    private ObjectMapper snakeCaseMapper(ObjectMapper objectMapper) {
        return cachedSnakeCaseMapper(objectMapper);
    }

    static ObjectMapper cachedSnakeCaseMapper(ObjectMapper objectMapper) {
        synchronized (SNAKE_CASE_MAPPERS) {
            return SNAKE_CASE_MAPPERS.computeIfAbsent(
                    objectMapper,
                    mapper -> mapper.copy().setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE)
            );
        }
    }

    private TagField buildTagField(SchemaField field) {
        return ",".equals(field.separator()) ? new TagField() : new TagField(field.separator().charAt(0));
    }

    private long executeNativeNumericUpdate(
            ValkeyClientRouting clientRouting,
            ValkeyObservationInvoker observationInvoker,
            String indexName,
            String key,
            NumericUpdateOperation operation) throws Exception {
        validateNumericField(operation.fieldName());
        String delta = String.valueOf(operation.signedDelta());
        String[] command = isIntegral(operation.delta())
                ? new String[]{"HINCRBY", key, operation.fieldName(), delta}
                : new String[]{"HINCRBYFLOAT", key, operation.fieldName(), delta};
        observationInvoker.execute(command[0], indexName, () -> command[0] + " " + key + " " + operation.fieldName() + " " + delta, "write", () -> clientRouting.executeWrite(command));
        return 1L;
    }

    private long executeScriptedUpdate(
            ValkeyClientRouting clientRouting,
            ValkeyObservationInvoker observationInvoker,
            String indexName,
            String key,
            List<UpdateOperation> operations,
            SearchPredicate predicate,
            ObjectMapper objectMapper) throws Exception {
        ConditionScript conditionScript = buildConditionScript(predicate, objectMapper);
        String script = String.format("""
                local key = KEYS[1]
                if redis.call('EXISTS', key) == 0 then
                    return 0
                end
                if not (%s) then
                    return 0
                end
                local index = %d
                local operationCount = tonumber(ARGV[index])
                index = index + 1
                for i = 1, operationCount do
                    local opType = ARGV[index]
                    local field = ARGV[index + 1]
                    local value = ARGV[index + 2]
                    index = index + 3
                    if opType == 'SET' then
                        redis.call('HSET', key, field, value)
                    elseif opType == 'INCREMENT' then
                        redis.call('HINCRBY', key, field, value)
                    elseif opType == 'DECREMENT' then
                        redis.call('HINCRBY', key, field, '-' .. value)
                    elseif opType == 'INCREMENT_FLOAT' then
                        redis.call('HINCRBYFLOAT', key, field, value)
                    elseif opType == 'DECREMENT_FLOAT' then
                        redis.call('HINCRBYFLOAT', key, field, '-' .. value)
                    else
                        error('Unsupported update operation type: ' .. opType)
                    end
                end
                return 1
                """.trim(), conditionScript.expression(), conditionScript.arguments().size() + 1);
        List<String> keysAndArgs = new java.util.ArrayList<>();
        keysAndArgs.add(key);
        keysAndArgs.addAll(conditionScript.arguments());
        keysAndArgs.add(Integer.toString(operations.size()));
        for (UpdateOperation operation : operations) {
            keysAndArgs.add(operationCode(operation));
            keysAndArgs.add(operation.fieldName());
            keysAndArgs.add(operationValue(operation, objectMapper));
        }
        Object result = scriptExecutor.executeWrite(
                clientRouting,
                observationInvoker,
                indexName,
                () -> "EVALSHA <hash-update> 1 " + key,
                script,
                1,
                keysAndArgs
        );
        return toLong(result);
    }

    private String serializeValue(Object value, ObjectMapper objectMapper) throws Exception {
        if (value instanceof CharSequence || value instanceof Number || value instanceof Boolean) {
            return String.valueOf(value);
        }
        return objectMapper.writeValueAsString(value);
    }

    private long toLong(Object value) {
        if (value instanceof Number number) {
            return number.longValue();
        }
        return Long.parseLong(String.valueOf(value));
    }

    private ConditionScript buildConditionScript(SearchPredicate predicate, ObjectMapper objectMapper) throws Exception {
        if (predicate == null) {
            return new ConditionScript("true", List.of());
        }
        List<String> arguments = new java.util.ArrayList<>();
        String expression = buildConditionExpression(predicate, arguments, objectMapper);
        return new ConditionScript(expression, arguments);
    }

    private String buildConditionExpression(SearchPredicate predicate, List<String> arguments, ObjectMapper objectMapper) throws Exception {
        if (predicate instanceof SearchPredicate.ExactMatch exactMatch) {
            int fieldIndex = arguments.size() + 1;
            arguments.add(exactMatch.fieldName());
            int expectedIndex = arguments.size() + 1;
            arguments.add(serializeValue(exactMatch.expectedValue(), objectMapper));
            return "(redis.call('HGET', key, ARGV[" + fieldIndex + "]) == ARGV[" + expectedIndex + "])";
        }
        if (predicate instanceof SearchPredicate.And and) {
            return "(" + buildConditionExpression(and.left(), arguments, objectMapper)
                    + " and "
                    + buildConditionExpression(and.right(), arguments, objectMapper) + ")";
        }
        if (predicate instanceof SearchPredicate.Or or) {
            return "(" + buildConditionExpression(or.left(), arguments, objectMapper)
                    + " or "
                    + buildConditionExpression(or.right(), arguments, objectMapper) + ")";
        }
        throw new IllegalArgumentException("Unsupported update predicate: " + predicate);
    }

    private record ConditionScript(String expression, List<String> arguments) {
    }

    private List<UpdateAssignment> requireAssignments(List<UpdateOperation> operations) {
        List<UpdateAssignment> assignments = new java.util.ArrayList<>(operations.size());
        for (UpdateOperation operation : operations) {
            if (!(operation instanceof UpdateAssignment assignment)) {
                throw new IllegalArgumentException("Numeric update operations require atomic script execution");
            }
            assignments.add(assignment);
        }
        return assignments;
    }

    private boolean containsNumericOperation(List<UpdateOperation> operations) {
        for (UpdateOperation operation : operations) {
            if (operation instanceof NumericUpdateOperation) {
                return true;
            }
        }
        return false;
    }

    private void validateNumericField(String fieldName) {
        SchemaField field = resolveField(fieldName);
        if (field.type() != FieldType.NUMERIC) {
            throw new IllegalArgumentException("increment/decrement only supports numeric schema fields: " + fieldName);
        }
    }

    private SchemaField resolveField(String fieldName) {
        for (SchemaField field : schema.fields()) {
            if (field.fieldName().equals(fieldName)) {
                return field;
            }
        }
        throw new IllegalArgumentException("Unknown schema field for partial update: " + fieldName);
    }

    private String operationCode(UpdateOperation operation) {
        if (operation instanceof UpdateAssignment) {
            return "SET";
        }
        NumericUpdateOperation numeric = (NumericUpdateOperation) operation;
        if (numeric.kind() == UpdateOperationKind.INCREMENT) {
            return isIntegral(numeric.delta()) ? "INCREMENT" : "INCREMENT_FLOAT";
        }
        return isIntegral(numeric.delta()) ? "DECREMENT" : "DECREMENT_FLOAT";
    }

    private String operationValue(UpdateOperation operation, ObjectMapper objectMapper) throws Exception {
        if (operation instanceof UpdateAssignment assignment) {
            return serializeValue(assignment.value(), objectMapper);
        }
        NumericUpdateOperation numeric = (NumericUpdateOperation) operation;
        validateNumericField(numeric.fieldName());
        return String.valueOf(numeric.delta());
    }

    private boolean isIntegral(Number value) {
        return value instanceof Byte || value instanceof Short || value instanceof Integer || value instanceof Long
                || new java.math.BigDecimal(String.valueOf(value)).stripTrailingZeros().scale() <= 0;
    }
}
