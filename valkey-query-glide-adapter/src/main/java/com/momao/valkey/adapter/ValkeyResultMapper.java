package com.momao.valkey.adapter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.momao.valkey.core.SearchResult;
import com.momao.valkey.core.metadata.IndexSchema;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class ValkeyResultMapper<T> {

    private final IndexSchema schema;

    private final Class<T> entityClass;

    private final ObjectMapper objectMapper;

    private final ValkeyEntityOperations<T> entityOperations;

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

        if (response.length == 2 && response[1] instanceof Map<?, ?> documents) {
            collectMappedDocuments(records, documents);
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
            throw new IllegalStateException("结果映射失败: " + entityClass.getName(), exception);
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

        for (int index = 0; index + 1 < fieldsArray.length; index += 2) {
            docMap.put(asString(fieldsArray[index]), normalizeValue(fieldsArray[index + 1]));
        }
        return docMap;
    }

    private void injectDocumentId(T entity, String docKey) {
        if (docKey == null || !docKey.startsWith(schema.prefix())) {
            return;
        }

        String id = docKey.substring(schema.prefix().length());
        if (writeIdWithSetter(entity, id)) {
            return;
        }
        writeIdWithField(entity, id);
    }

    private boolean writeIdWithSetter(T entity, String id) {
        try {
            Method setter = entityClass.getMethod("setId", String.class);
            setter.invoke(entity, id);
            return true;
        } catch (ReflectiveOperationException ignored) {
            return false;
        }
    }

    private void writeIdWithField(T entity, String id) {
        Class<?> type = entityClass;
        while (type != null && type != Object.class) {
            try {
                Field field = type.getDeclaredField("id");
                field.setAccessible(true);
                field.set(entity, convertId(id, field.getType()));
                return;
            } catch (NoSuchFieldException ignored) {
                type = type.getSuperclass();
            } catch (ReflectiveOperationException exception) {
                throw new IllegalStateException("回填实体 ID 失败: " + entityClass.getName(), exception);
            }
        }
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

}
