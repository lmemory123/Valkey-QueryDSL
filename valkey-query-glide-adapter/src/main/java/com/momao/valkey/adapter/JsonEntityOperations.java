package com.momao.valkey.adapter;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.JsonNodeFactory;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.momao.valkey.adapter.observability.ValkeyObservationInvoker;
import com.momao.valkey.annotation.FieldType;
import com.momao.valkey.core.NumericUpdateOperation;
import com.momao.valkey.core.SearchPredicate;
import com.momao.valkey.core.UpdateOperation;
import com.momao.valkey.core.UpdateAssignment;
import com.momao.valkey.core.UpdateOperationKind;
import com.momao.valkey.core.metadata.SchemaField;
import com.momao.valkey.core.metadata.IndexSchema;
import glide.api.models.commands.FT.FTCreateOptions;
import glide.api.models.commands.FT.FTCreateOptions.FieldInfo;
import glide.api.models.commands.FT.FTCreateOptions.NumericField;
import glide.api.models.commands.FT.FTCreateOptions.TagField;
import glide.api.models.commands.FT.FTCreateOptions.TextField;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

final class JsonEntityOperations<T> implements ValkeyEntityOperations<T> {

    private final IndexSchema schema;

    private final ValkeyScriptExecutor scriptExecutor;

    JsonEntityOperations() {
        this(new IndexSchema("idx:json", com.momao.valkey.annotation.StorageType.JSON, List.of(), List.of()));
    }

    JsonEntityOperations(IndexSchema schema) {
        this(schema, ValkeyScriptExecutor.DEFAULT);
    }

    JsonEntityOperations(IndexSchema schema, ValkeyScriptExecutor scriptExecutor) {
        this.schema = schema;
        this.scriptExecutor = scriptExecutor;
    }

    @Override
    public String[] buildSaveCommand(String key, T entity, ObjectMapper objectMapper) throws Exception {
        JsonNode tree = objectMapper.valueToTree(entity);
        normalizeCollectionTagsForStorage(tree);
        normalizeVectorFieldsForStorage(tree);
        return new String[]{"JSON.SET", key, "$", objectMapper.writeValueAsString(tree)};
    }

    @Override
    public void save(
            ValkeyClientRouting clientRouting,
            ValkeyObservationInvoker observationInvoker,
            String indexName,
            String key,
            T entity,
            ObjectMapper objectMapper) throws Exception {
        String[] command = buildSaveCommand(key, entity, objectMapper);
        observationInvoker.execute("JSON.SET", indexName, () -> "JSON.SET " + key + " $ <json>", "write", () -> clientRouting.executeWrite(command));
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
        for (UpdateAssignment assignment : assignments) {
            SchemaField field = resolveFieldOrDynamic(assignment.fieldName());
            String path = "$." + normalizeUpdatePath(field);
            String payload = objectMapper.writeValueAsString(normalizeUpdateValue(field, assignment.value(), objectMapper));
            String[] command = new String[]{"JSON.SET", key, path, payload};
            observationInvoker.execute("JSON.SET", indexName, () -> "JSON.SET " + key + " " + path + " <json>", "write", () -> clientRouting.executeWrite(command));
        }
        return 1L;
    }

    @Override
    public Map<String, String> toStorageFields(T entity, ObjectMapper objectMapper) {
        throw new UnsupportedOperationException("JSON 存储不支持转换为 Hash 字段映射");
    }

    @Override
    public T readEntity(ObjectMapper objectMapper, Class<T> entityClass, Map<String, ?> storedFields) throws Exception {
        Object rawJson = storedFields.get("$");
        JsonNode tree = rawJson == null
                ? rebuildProjectedTree(storedFields, objectMapper)
                : objectMapper.readTree(String.valueOf(rawJson));
        restoreCollectionTagsForRead(tree, objectMapper);
        restoreVectorFieldsForRead(tree, objectMapper);
        return objectMapper.treeToValue(tree, entityClass);
    }

    @Override
    public FTCreateOptions.DataType dataType() {
        return FTCreateOptions.DataType.JSON;
    }

    @Override
    public FieldInfo toFieldInfo(SchemaField field) {
        String reference = "$." + field.jsonPath();
        return switch (field.type()) {
            case TEXT -> new FieldInfo(reference, field.fieldName(), new TextField());
            case TAG -> new FieldInfo(reference, field.fieldName(), buildTagField(field));
            case NUMERIC -> new FieldInfo(reference, field.fieldName(), new NumericField());
            case VECTOR -> throw new UnsupportedOperationException("Vector schema field info should be emitted through raw FT.CREATE command generation");
        };
    }

    private TagField buildTagField(SchemaField field) {
        return ",".equals(field.separator()) ? new TagField() : new TagField(field.separator().charAt(0));
    }

    private SchemaField resolveField(String fieldName) {
        for (SchemaField field : schema.fields()) {
            if (field.fieldName().equals(fieldName)) {
                return field;
            }
        }
        throw new IllegalArgumentException("Unknown schema field for partial update: " + fieldName);
    }

    private SchemaField resolveFieldOrDynamic(String fieldName) {
        for (SchemaField field : schema.fields()) {
            if (field.fieldName().equals(fieldName)) {
                return field;
            }
        }
        return new SchemaField(fieldName, fieldName, FieldType.TEXT, false, 1.0d, false, ",");
    }

    private String normalizeUpdatePath(SchemaField field) {
        if (isCollectionTagField(field)) {
            return collectionFieldPath(field);
        }
        String jsonPath = field.jsonPath();
        return jsonPath.startsWith("$.") ? jsonPath.substring(2) : jsonPath;
    }

    private JsonNode normalizeUpdateValue(SchemaField field, Object value, ObjectMapper objectMapper) {
        if (isCollectionTagField(field)) {
            return JsonNodeFactory.instance.textNode(joinCollectionValues(field.separator(), value));
        }
        if (field.type() == FieldType.VECTOR) {
            return JsonNodeFactory.instance.textNode(vectorLiteral(value));
        }
        return objectMapper.valueToTree(value);
    }

    private void normalizeVectorFieldsForStorage(JsonNode tree) {
        if (!(tree instanceof ObjectNode root)) {
            return;
        }
        for (SchemaField field : schema.fields()) {
            if (field.type() != FieldType.VECTOR) {
                continue;
            }
            JsonNode value = getNode(root, normalizeUpdatePath(field));
            if (value == null || value.isNull() || value.isTextual()) {
                continue;
            }
            setTextValue(root, normalizeUpdatePath(field), vectorLiteral(value));
        }
    }

    private void restoreVectorFieldsForRead(JsonNode tree, ObjectMapper objectMapper) throws Exception {
        if (!(tree instanceof ObjectNode root)) {
            return;
        }
        for (SchemaField field : schema.fields()) {
            if (field.type() != FieldType.VECTOR) {
                continue;
            }
            JsonNode value = getNode(root, normalizeUpdatePath(field));
            if (value == null || value.isNull() || !value.isTextual()) {
                continue;
            }
            setNode(root, normalizeUpdatePath(field), objectMapper.readTree(value.asText()));
        }
    }

    private void normalizeCollectionTagsForStorage(JsonNode tree) {
        if (!(tree instanceof ObjectNode root)) {
            return;
        }
        for (SchemaField field : schema.fields()) {
            if (!isCollectionTagField(field)) {
                continue;
            }
            JsonNode value = getNode(root, collectionFieldPath(field));
            if (!(value instanceof ArrayNode arrayNode)) {
                continue;
            }
            List<String> parts = new ArrayList<>();
            for (JsonNode element : arrayNode) {
                if (!element.isNull()) {
                    parts.add(element.asText());
                }
            }
            setTextValue(root, collectionFieldPath(field), String.join(field.separator(), parts));
        }
    }

    private void restoreCollectionTagsForRead(JsonNode tree, ObjectMapper objectMapper) {
        if (!(tree instanceof ObjectNode root)) {
            return;
        }
        for (SchemaField field : schema.fields()) {
            if (!isCollectionTagField(field)) {
                continue;
            }
            JsonNode value = getNode(root, collectionFieldPath(field));
            if (value == null || value.isNull() || value.isArray()) {
                continue;
            }
            ArrayNode arrayNode = objectMapper.createArrayNode();
            String raw = value.asText();
            if (!raw.isBlank()) {
                for (String item : raw.split(java.util.regex.Pattern.quote(field.separator()))) {
                    String trimmed = item.trim();
                    if (!trimmed.isEmpty()) {
                        arrayNode.add(trimmed);
                    }
                }
            }
            setNode(root, collectionFieldPath(field), arrayNode);
        }
    }

    private boolean isCollectionTagField(SchemaField field) {
        return field.type() == com.momao.valkey.annotation.FieldType.TAG && field.jsonPath().endsWith("[*]");
    }

    private String collectionFieldPath(SchemaField field) {
        return field.jsonPath().substring(0, field.jsonPath().length() - 3);
    }

    private String joinCollectionValues(String separator, Object value) {
        List<String> parts = new ArrayList<>();
        if (value instanceof Iterable<?> iterable) {
            for (Object item : iterable) {
                if (item != null) {
                    parts.add(String.valueOf(item));
                }
            }
            return String.join(separator, parts);
        }
        if (value != null && value.getClass().isArray()) {
            int length = java.lang.reflect.Array.getLength(value);
            for (int index = 0; index < length; index++) {
                Object item = java.lang.reflect.Array.get(value, index);
                if (item != null) {
                    parts.add(String.valueOf(item));
                }
            }
            return String.join(separator, parts);
        }
        return String.valueOf(value);
    }

    private String vectorLiteral(Object value) {
        if (value instanceof JsonNode jsonNode) {
            return jsonNode.toString();
        }
        if (value instanceof float[] floats) {
            StringBuilder builder = new StringBuilder("[");
            for (int index = 0; index < floats.length; index++) {
                if (index > 0) {
                    builder.append(',');
                }
                builder.append(Float.toString(floats[index]));
            }
            return builder.append(']').toString();
        }
        if (value instanceof double[] doubles) {
            StringBuilder builder = new StringBuilder("[");
            for (int index = 0; index < doubles.length; index++) {
                if (index > 0) {
                    builder.append(',');
                }
                builder.append(Double.toString(doubles[index]));
            }
            return builder.append(']').toString();
        }
        return String.valueOf(value);
    }

    private long executeNativeNumericUpdate(
            ValkeyClientRouting clientRouting,
            ValkeyObservationInvoker observationInvoker,
            String indexName,
            String key,
            NumericUpdateOperation operation) throws Exception {
        SchemaField field = resolveNumericField(operation.fieldName());
        String path = "$." + normalizeUpdatePath(field);
        String delta = String.valueOf(operation.signedDelta());
        String[] command = new String[]{"JSON.NUMINCRBY", key, path, delta};
        observationInvoker.execute("JSON.NUMINCRBY", indexName, () -> "JSON.NUMINCRBY " + key + " " + path + " " + delta, "write", () -> clientRouting.executeWrite(command));
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
                    local path = ARGV[index + 1]
                    local value = ARGV[index + 2]
                    index = index + 3
                    if opType == 'SET' then
                        redis.call('JSON.SET', key, path, value)
                    elseif opType == 'INCREMENT' then
                        redis.call('JSON.NUMINCRBY', key, path, value)
                    elseif opType == 'DECREMENT' then
                        redis.call('JSON.NUMINCRBY', key, path, '-' .. value)
                    else
                        error('Unsupported update operation type: ' .. opType)
                    end
                end
                return 1
                """.trim(), conditionScript.expression(), conditionScript.arguments().size() + 1);
        List<String> keysAndArgs = new ArrayList<>();
        keysAndArgs.add(key);
        keysAndArgs.addAll(conditionScript.arguments());
        keysAndArgs.add(Integer.toString(operations.size()));
        for (UpdateOperation operation : operations) {
            keysAndArgs.add(operation instanceof UpdateAssignment ? "SET" : operation.kind().name());
            keysAndArgs.add(operationPath(operation));
            keysAndArgs.add(operationValue(operation, objectMapper));
        }
        Object result = scriptExecutor.executeWrite(
                clientRouting,
                observationInvoker,
                indexName,
                () -> "EVALSHA <json-update> 1 " + key,
                script,
                1,
                keysAndArgs
        );
        return toLong(result);
    }

    private ConditionScript buildConditionScript(SearchPredicate predicate, ObjectMapper objectMapper) throws Exception {
        if (predicate == null) {
            return new ConditionScript("true", List.of());
        }
        List<String> arguments = new ArrayList<>();
        String expression = buildConditionExpression(predicate, arguments, objectMapper);
        return new ConditionScript(expression, arguments);
    }

    private String buildConditionExpression(SearchPredicate predicate, List<String> arguments, ObjectMapper objectMapper) throws Exception {
        if (predicate instanceof SearchPredicate.ExactMatch exactMatch) {
            SchemaField field = resolveFieldOrDynamic(exactMatch.fieldName());
            int pathIndex = arguments.size() + 1;
            arguments.add(buildExpectationPath(field));
            int expectedIndex = arguments.size() + 1;
            arguments.add(objectMapper.writeValueAsString(normalizeUpdateValue(field, exactMatch.expectedValue(), objectMapper)));
            return "(redis.call('JSON.GET', key, ARGV[" + pathIndex + "]) == ARGV[" + expectedIndex + "])";
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

    private String buildExpectationPath(SchemaField field) {
        return "." + normalizeUpdatePath(field);
    }

    private JsonNode getNode(ObjectNode root, String path) {
        JsonNode current = root;
        for (String segment : path.split("\\.")) {
            if (current == null || !current.isObject()) {
                return null;
            }
            current = current.get(segment);
        }
        return current;
    }

    private void setTextValue(ObjectNode root, String path, String value) {
        setNode(root, path, root.textNode(value));
    }

    private void setNode(ObjectNode root, String path, JsonNode value) {
        String[] segments = path.split("\\.");
        ObjectNode current = root;
        for (int index = 0; index < segments.length - 1; index++) {
            JsonNode child = current.get(segments[index]);
            ObjectNode objectChild;
            if (child instanceof ObjectNode existingObject) {
                objectChild = existingObject;
            } else {
                objectChild = current.objectNode();
                current.set(segments[index], objectChild);
            }
            current = objectChild;
        }
        current.set(segments[segments.length - 1], value);
    }

    private JsonNode rebuildProjectedTree(Map<String, ?> storedFields, ObjectMapper objectMapper) {
        ObjectNode root = objectMapper.createObjectNode();
        for (SchemaField field : schema.fields()) {
            Object rawValue = storedFields.get(field.fieldName());
            if (rawValue == null) {
                continue;
            }
            setNode(root, normalizeUpdatePath(field), parseProjectedValue(rawValue, objectMapper));
        }
        return root;
    }

    private JsonNode parseProjectedValue(Object rawValue, ObjectMapper objectMapper) {
        if (rawValue instanceof JsonNode jsonNode) {
            return jsonNode;
        }
        String normalized = String.valueOf(rawValue);
        try {
            return objectMapper.readTree(normalized);
        } catch (Exception ignored) {
            return objectMapper.getNodeFactory().textNode(normalized);
        }
    }

    private long toLong(Object value) {
        if (value instanceof Number number) {
            return number.longValue();
        }
        return Long.parseLong(String.valueOf(value));
    }

    private record ConditionScript(String expression, List<String> arguments) {
    }

    private List<UpdateAssignment> requireAssignments(List<UpdateOperation> operations) {
        List<UpdateAssignment> assignments = new ArrayList<>(operations.size());
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

    private SchemaField resolveNumericField(String fieldName) {
        SchemaField field = resolveField(fieldName);
        if (field.type() != FieldType.NUMERIC) {
            throw new IllegalArgumentException("increment/decrement only supports numeric schema fields: " + fieldName);
        }
        return field;
    }

    private String operationPath(UpdateOperation operation) {
        SchemaField field = operation instanceof NumericUpdateOperation numeric
                ? resolveNumericField(numeric.fieldName())
                : resolveFieldOrDynamic(operation.fieldName());
        return "$." + normalizeUpdatePath(field);
    }

    private String operationValue(UpdateOperation operation, ObjectMapper objectMapper) throws Exception {
        if (operation instanceof UpdateAssignment assignment) {
            SchemaField field = resolveFieldOrDynamic(assignment.fieldName());
            return objectMapper.writeValueAsString(normalizeUpdateValue(field, assignment.value(), objectMapper));
        }
        NumericUpdateOperation numeric = (NumericUpdateOperation) operation;
        resolveNumericField(numeric.fieldName());
        return String.valueOf(numeric.delta());
    }
}
