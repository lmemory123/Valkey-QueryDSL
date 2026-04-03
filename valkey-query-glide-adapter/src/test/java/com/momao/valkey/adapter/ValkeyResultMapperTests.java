package com.momao.valkey.adapter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.momao.valkey.annotation.StorageType;
import com.momao.valkey.core.SearchResult;
import com.momao.valkey.core.metadata.IndexSchema;
import com.momao.valkey.core.metadata.SchemaField;
import glide.api.models.commands.FT.FTCreateOptions;
import glide.api.models.commands.FT.FTCreateOptions.FieldInfo;
import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ValkeyResultMapperTests {

    @Test
    void fillsIdWhenDocumentMatchesAnyConfiguredPrefix() {
        ValkeyResultMapper<MultiPrefixEntity> mapper = new ValkeyResultMapper<>(
                new IndexSchema("idx:test", StorageType.HASH, List.of("student:", "student:archived:"), List.of(SchemaField.tag("id", ",", false))),
                MultiPrefixEntity.class,
                new ObjectMapper().findAndRegisterModules(),
                new StubEntityOperations<>()
        );

        SearchResult<MultiPrefixEntity> result = mapper.mapSearchResponse(new Object[]{
                1L,
                Map.of("student:archived:42", Map.of("name", "Alice"))
        });

        assertEquals(1, result.records().size());
        assertEquals("42", result.records().get(0).getId());
    }

    @Test
    void fillsIdThroughNonStringSetterType() {
        ValkeyResultMapper<LongIdEntity> mapper = new ValkeyResultMapper<>(
                new IndexSchema("idx:test", StorageType.HASH, List.of("student:"), List.of(SchemaField.tag("id", ",", false))),
                LongIdEntity.class,
                new ObjectMapper().findAndRegisterModules(),
                new StubEntityOperations<>()
        );

        SearchResult<LongIdEntity> result = mapper.mapSearchResponse(new Object[]{
                1L,
                Map.of("student:7", Map.of("name", "Bob"))
        });

        assertEquals(1, result.records().size());
        assertEquals(7L, result.records().get(0).getId());
    }

    private static final class StubEntityOperations<T> implements ValkeyEntityOperations<T> {

        @Override
        public String[] buildSaveCommand(String key, T entity, ObjectMapper objectMapper) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void save(ValkeyClientRouting clientRouting, com.momao.valkey.adapter.observability.ValkeyObservationInvoker observationInvoker, String indexName, String key, T entity, ObjectMapper objectMapper) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long update(ValkeyClientRouting clientRouting, com.momao.valkey.adapter.observability.ValkeyObservationInvoker observationInvoker, String indexName, String key, List<com.momao.valkey.core.UpdateOperation> operations, com.momao.valkey.core.SearchPredicate predicate, ObjectMapper objectMapper) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Map<String, String> toStorageFields(T entity, ObjectMapper objectMapper) {
            return Map.of();
        }

        @Override
        public T readEntity(ObjectMapper objectMapper, Class<T> entityClass, Map<String, ?> storedFields) {
            return objectMapper.convertValue(new LinkedHashMap<>(storedFields), entityClass);
        }

        @Override
        public FTCreateOptions.DataType dataType() {
            return FTCreateOptions.DataType.HASH;
        }

        @Override
        public FieldInfo toFieldInfo(SchemaField field) {
            throw new UnsupportedOperationException();
        }
    }

    public static class MultiPrefixEntity {

        private String id;
        private String name;

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }

    public static class LongIdEntity {

        private long id;
        private String name;

        public long getId() {
            return id;
        }

        public void setId(long id) {
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }
}
