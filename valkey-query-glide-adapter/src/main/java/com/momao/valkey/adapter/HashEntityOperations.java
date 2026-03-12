package com.momao.valkey.adapter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategies;
import com.momao.valkey.core.metadata.SchemaField;
import glide.api.GlideClient;
import glide.api.models.commands.FT.FTCreateOptions;
import glide.api.models.commands.FT.FTCreateOptions.FieldInfo;
import glide.api.models.commands.FT.FTCreateOptions.NumericField;
import glide.api.models.commands.FT.FTCreateOptions.TagField;
import glide.api.models.commands.FT.FTCreateOptions.TextField;

import java.util.LinkedHashMap;
import java.util.Map;

final class HashEntityOperations<T> implements ValkeyEntityOperations<T> {

    @Override
    public void save(GlideClient client, String key, T entity, ObjectMapper objectMapper) throws Exception {
        client.hset(key, toStorageFields(entity, objectMapper)).get();
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
        };
    }

    private ObjectMapper snakeCaseMapper(ObjectMapper objectMapper) {
        return objectMapper.copy().setPropertyNamingStrategy(PropertyNamingStrategies.SNAKE_CASE);
    }

    private TagField buildTagField(SchemaField field) {
        return ",".equals(field.separator()) ? new TagField() : new TagField(field.separator().charAt(0));
    }
}
