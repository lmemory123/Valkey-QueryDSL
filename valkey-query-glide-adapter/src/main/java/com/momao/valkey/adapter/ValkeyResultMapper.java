package com.momao.valkey.adapter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.momao.valkey.core.SearchResult;
import com.momao.valkey.core.exception.ValkeyErrorCode;
import com.momao.valkey.core.exception.ValkeyResultMappingException;
import com.momao.valkey.core.metadata.IndexSchema;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

final class ValkeyResultMapper<T> {

    private final IndexSchema schema;

    private final Class<T> entityClass;

    private final ObjectMapper objectMapper;

    private final ValkeyEntityOperations<T> entityOperations;

    private volatile IdAccessor idAccessor;

    ValkeyResultMapper(
            IndexSchema schema,
            Class<T> entityClass,
            ObjectMapper objectMapper,
            ValkeyEntityOperations<T> entityOperations) {
        this.schema = schema;
        this.entityClass = entityClass;
        this.objectMapper = objectMapper;
        this.entityOperations = entityOperations;
    }

    SearchResult<T> mapSearchResponse(Object[] response) {
        if (response == null || response.length == 0) {
            return new SearchResult<>(0L, List.of());
        }

        long total = toLong(response[0]);
        List<T> records = new ArrayList<>();
        if (response.length == 1) {
            return new SearchResult<>(total, records);
        }

        if (response.length == 2 && collectTopLevelDocuments(records, response[1])) {
            return new SearchResult<>(total, records);
        }

        if (looksLikeTupleEntries(response)) {
            for (int index = 1; index < response.length; index++) {
                collectTupleDocument(records, response[index]);
            }
            return new SearchResult<>(total, records);
        }

        for (int index = 1; index + 1 < response.length; index += 2) {
            T entity = toEntity(String.valueOf(response[index]), response[index + 1]);
            if (entity != null) {
                records.add(entity);
            }
        }
        return new SearchResult<>(total, records);
    }

    private void collectMappedDocuments(List<T> records, Map<?, ?> documents) {
        for (Map.Entry<?, ?> entry : documents.entrySet()) {
            T entity = toEntity(asString(entry.getKey()), entry.getValue());
            if (entity != null) {
                records.add(entity);
            }
        }
    }

    private boolean collectTopLevelDocuments(List<T> records, Object candidate) {
        if (candidate instanceof Map<?, ?> documents) {
            collectMappedDocuments(records, documents);
            return true;
        }

        Object[] values = toArray(candidate);
        if (values.length == 0) {
            return false;
        }
        if (isTupleEntry(values)) {
            collectTupleDocument(records, values);
            return true;
        }

        boolean handled = false;
        for (Object value : values) {
            if (isTupleEntry(value)) {
                collectTupleDocument(records, value);
                handled = true;
            }
        }
        return handled;
    }

    private boolean looksLikeTupleEntries(Object[] response) {
        if (response.length <= 1) {
            return false;
        }
        for (int index = 1; index < response.length; index++) {
            if (!isTupleEntry(response[index])) {
                return false;
            }
        }
        return true;
    }

    private void collectTupleDocument(List<T> records, Object tupleCandidate) {
        Object[] tuple = toArray(tupleCandidate);
        if (tuple.length != 2) {
            return;
        }
        T entity = toEntity(asString(tuple[0]), tuple[1]);
        if (entity != null) {
            records.add(entity);
        }
    }

    private boolean isTupleEntry(Object candidate) {
        Object[] tuple = toArray(candidate);
        if (tuple.length != 2) {
            return false;
        }
        Object key = tuple[0];
        return !(key instanceof Map<?, ?>) && !(key instanceof List<?>) && !(key instanceof Object[]);
    }

    private T toEntity(String docKey, Object rawDocument) {
        try {
            Map<String, Object> storedFields = toFieldMap(rawDocument);
            T entity = entityOperations.readEntity(objectMapper, entityClass, storedFields);
            if (entity == null) {
                return null;
            }
            injectDocumentId(entity, docKey);
            return entity;
        } catch (Exception exception) {
            throw new ValkeyResultMappingException(ValkeyErrorCode.RESULT_MAPPING_ERROR, "结果映射失败: " + entityClass.getName(), exception);
        }
    }

    private Map<String, Object> toFieldMap(Object rawDocument) {
        Map<String, Object> docMap = new LinkedHashMap<>();
        if (rawDocument instanceof Map<?, ?> rawFields) {
            for (Map.Entry<?, ?> entry : rawFields.entrySet()) {
                docMap.put(asString(entry.getKey()), normalizeValue(entry.getValue()));
            }
            return docMap;
        }

        Object[] fieldsArray = rawDocument instanceof Object[] values ? values : null;
        if (fieldsArray == null && rawDocument instanceof List<?> values) {
            fieldsArray = values.toArray();
        }
        if (fieldsArray == null) {
            return docMap;
        }

        if (fieldsArray.length == 1) {
            Object[] nested = toArray(fieldsArray[0]);
            if (nested.length > 0) {
                fieldsArray = nested;
            }
        }

        for (int index = 0; index + 1 < fieldsArray.length; index += 2) {
            docMap.put(asString(fieldsArray[index]), normalizeValue(fieldsArray[index + 1]));
        }
        return docMap;
    }

    private void injectDocumentId(T entity, String docKey) {
        String id = extractId(docKey);
        if (id == null) {
            return;
        }
        writeId(entity, id);
    }

    private String extractId(String docKey) {
        if (docKey == null) {
            return null;
        }
        String matchedPrefix = null;
        for (String prefix : schema.prefixes()) {
            if (prefix != null && !prefix.isEmpty() && docKey.startsWith(prefix)
                    && (matchedPrefix == null || prefix.length() > matchedPrefix.length())) {
                matchedPrefix = prefix;
            }
        }
        if (matchedPrefix != null) {
            return docKey.substring(matchedPrefix.length());
        }
        String defaultPrefix = schema.prefix();
        if (defaultPrefix != null && !defaultPrefix.isEmpty() && docKey.startsWith(defaultPrefix)) {
            return docKey.substring(defaultPrefix.length());
        }
        return null;
    }

    private void writeId(T entity, String id) {
        IdAccessor accessor = resolveIdAccessor();
        if (accessor == null) {
            return;
        }
        try {
            accessor.write(entity, convertId(id, accessor.targetType()));
        } catch (ReflectiveOperationException exception) {
            throw new ValkeyResultMappingException(ValkeyErrorCode.RESULT_ENTITY_ID_FILL_FAILED, "回填实体 ID 失败: " + entityClass.getName(), exception);
        }
    }

    private IdAccessor resolveIdAccessor() {
        IdAccessor cached = idAccessor;
        if (cached != null) {
            return cached.isMissing() ? null : cached;
        }
        synchronized (this) {
            cached = idAccessor;
            if (cached != null) {
                return cached.isMissing() ? null : cached;
            }
            idAccessor = discoverIdAccessor();
            return idAccessor.isMissing() ? null : idAccessor;
        }
    }

    private IdAccessor discoverIdAccessor() {
        Method setter = findIdSetter();
        if (setter != null) {
            return IdAccessor.forSetter(setter);
        }
        Field field = findIdField();
        if (field != null) {
            return IdAccessor.forField(field);
        }
        return IdAccessor.missing();
    }

    private Method findIdSetter() {
        Class<?> type = entityClass;
        while (type != null && type != Object.class) {
            try {
                Method setter = type.getDeclaredMethod("setId", String.class);
                setter.setAccessible(true);
                return setter;
            } catch (NoSuchMethodException ignored) {
                type = type.getSuperclass();
            }
        }
        type = entityClass;
        while (type != null && type != Object.class) {
            for (Method method : type.getDeclaredMethods()) {
                if ("setId".equals(method.getName()) && method.getParameterCount() == 1) {
                    method.setAccessible(true);
                    return method;
                }
            }
            type = type.getSuperclass();
        }
        return null;
    }

    private Field findIdField() {
        Class<?> type = entityClass;
        while (type != null && type != Object.class) {
            try {
                Field field = type.getDeclaredField("id");
                field.setAccessible(true);
                return field;
            } catch (NoSuchFieldException ignored) {
                type = type.getSuperclass();
            }
        }
        return null;
    }

    private Object convertId(String id, Class<?> targetType) {
        if (String.class.equals(targetType)) {
            return id;
        }
        if (Long.class.equals(targetType) || long.class.equals(targetType)) {
            return Long.parseLong(id);
        }
        if (Integer.class.equals(targetType) || int.class.equals(targetType)) {
            return Integer.parseInt(id);
        }
        return id;
    }

    private Object normalizeValue(Object value) {
        if (value instanceof byte[] bytes) {
            return new String(bytes, StandardCharsets.UTF_8);
        }
        if (value == null || value instanceof String || value instanceof Number || value instanceof Boolean) {
            return value;
        }
        if (value instanceof Map<?, ?> || value instanceof List<?> || value instanceof Object[]) {
            return value;
        }
        return String.valueOf(value);
    }

    private String asString(Object value) {
        if (value instanceof byte[] bytes) {
            return new String(bytes, StandardCharsets.UTF_8);
        }
        return String.valueOf(value);
    }

    private long toLong(Object value) {
        if (value instanceof Number number) {
            return number.longValue();
        }
        return Long.parseLong(String.valueOf(value));
    }

    private Object[] toArray(Object value) {
        if (value instanceof Object[] values) {
            return values;
        }
        if (value instanceof List<?> values) {
            return values.toArray();
        }
        return value == null ? new Object[0] : new Object[]{value};
    }

    private static final class IdAccessor {

        private final Method setter;

        private final Field field;

        private final Class<?> targetType;

        private final boolean missing;

        private IdAccessor(Method setter, Field field, Class<?> targetType, boolean missing) {
            this.setter = setter;
            this.field = field;
            this.targetType = targetType;
            this.missing = missing;
        }

        static IdAccessor forSetter(Method setter) {
            return new IdAccessor(Objects.requireNonNull(setter), null, setter.getParameterTypes()[0], false);
        }

        static IdAccessor forField(Field field) {
            return new IdAccessor(null, Objects.requireNonNull(field), field.getType(), false);
        }

        static IdAccessor missing() {
            return new IdAccessor(null, null, String.class, true);
        }

        boolean isMissing() {
            return missing;
        }

        Class<?> targetType() {
            return targetType;
        }

        void write(Object entity, Object value) throws ReflectiveOperationException {
            if (setter != null) {
                setter.invoke(entity, value);
                return;
            }
            if (field != null) {
                field.set(entity, value);
            }
        }
    }
}
