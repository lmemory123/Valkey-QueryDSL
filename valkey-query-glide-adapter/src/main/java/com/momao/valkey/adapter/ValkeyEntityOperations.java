package com.momao.valkey.adapter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.momao.valkey.core.metadata.SchemaField;
import glide.api.GlideClient;
import glide.api.models.commands.FT.FTCreateOptions;
import glide.api.models.commands.FT.FTCreateOptions.FieldInfo;

import java.util.Map;

interface ValkeyEntityOperations<T> {

    void save(GlideClient client, String key, T entity, ObjectMapper objectMapper) throws Exception;

    Map<String, String> toStorageFields(T entity, ObjectMapper objectMapper);

    T readEntity(ObjectMapper objectMapper, Class<T> entityClass, Map<String, ?> storedFields) throws Exception;

    FTCreateOptions.DataType dataType();

    FieldInfo toFieldInfo(SchemaField field);
}
