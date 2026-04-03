package com.momao.valkey.adapter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.momao.valkey.adapter.observability.ValkeyObservationInvoker;
import com.momao.valkey.core.SearchPredicate;
import com.momao.valkey.core.UpdateOperation;
import com.momao.valkey.core.metadata.SchemaField;
import glide.api.models.commands.FT.FTCreateOptions;
import glide.api.models.commands.FT.FTCreateOptions.FieldInfo;

import java.util.List;
import java.util.Map;

interface ValkeyEntityOperations<T> {

    String[] buildSaveCommand(String key, T entity, ObjectMapper objectMapper) throws Exception;

    void save(
            ValkeyClientRouting clientRouting,
            ValkeyObservationInvoker observationInvoker,
            String indexName,
            String key,
            T entity,
            ObjectMapper objectMapper) throws Exception;

    long update(
            ValkeyClientRouting clientRouting,
            ValkeyObservationInvoker observationInvoker,
            String indexName,
            String key,
            List<UpdateOperation> operations,
            SearchPredicate predicate,
            ObjectMapper objectMapper) throws Exception;

    Map<String, String> toStorageFields(T entity, ObjectMapper objectMapper);

    T readEntity(ObjectMapper objectMapper, Class<T> entityClass, Map<String, ?> storedFields) throws Exception;

    FTCreateOptions.DataType dataType();

    FieldInfo toFieldInfo(SchemaField field);
}
