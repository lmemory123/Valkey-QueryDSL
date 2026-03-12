package com.momao.valkey.adapter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.momao.valkey.core.metadata.SchemaField;
import glide.api.GlideClient;
import glide.api.models.commands.FT.FTCreateOptions;
import glide.api.models.commands.FT.FTCreateOptions.FieldInfo;
import glide.api.models.commands.FT.FTCreateOptions.NumericField;
import glide.api.models.commands.FT.FTCreateOptions.TagField;
import glide.api.models.commands.FT.FTCreateOptions.TextField;

import java.util.Map;

final class JsonEntityOperations<T> implements ValkeyEntityOperations<T> {

    @Override
    public void save(GlideClient client, String key, T entity, ObjectMapper objectMapper) throws Exception {
        client.customCommand(new String[]{"JSON.SET", key, "$", objectMapper.writeValueAsString(entity)}).get();
    }

    @Override
    public Map<String, String> toStorageFields(T entity, ObjectMapper objectMapper) {
        throw new UnsupportedOperationException("JSON 存储不支持转换为 Hash 字段映射");
    }

    @Override
    public T readEntity(ObjectMapper objectMapper, Class<T> entityClass, Map<String, ?> storedFields) throws Exception {
        Object rawJson = storedFields.get("$");
        if (rawJson == null) {
            return null;
        }
        return objectMapper.readValue(String.valueOf(rawJson), entityClass);
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
        };
    }

    private TagField buildTagField(SchemaField field) {
        return ",".equals(field.separator()) ? new TagField() : new TagField(field.separator().charAt(0));
    }
}
