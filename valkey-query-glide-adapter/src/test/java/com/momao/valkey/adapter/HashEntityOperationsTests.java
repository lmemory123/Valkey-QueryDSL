package com.momao.valkey.adapter;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.momao.valkey.annotation.StorageType;
import com.momao.valkey.core.metadata.IndexSchema;
import com.momao.valkey.core.metadata.SchemaField;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

class HashEntityOperationsTests {

    @Test
    void reusesCachedSnakeCaseMapperForSameBaseMapper() {
        ObjectMapper baseMapper = new ObjectMapper().findAndRegisterModules();

        ObjectMapper first = HashEntityOperations.cachedSnakeCaseMapper(baseMapper);
        ObjectMapper second = HashEntityOperations.cachedSnakeCaseMapper(baseMapper);

        assertSame(first, second);
    }

    @Test
    void mapsCamelCaseFieldsWithoutCreatingNewMapperEachCall() {
        HashEntityOperations<SampleEntity> operations = new HashEntityOperations<>(new IndexSchema(
                "idx:test",
                StorageType.HASH,
                List.of("test:"),
                List.of(SchemaField.tag("id", ",", false))
        ));
        ObjectMapper mapper = new ObjectMapper().findAndRegisterModules();
        SampleEntity entity = new SampleEntity();
        entity.setFirstName("Alice");

        assertEquals("Alice", operations.toStorageFields(entity, mapper).get("first_name"));
        assertEquals("Alice", operations.toStorageFields(entity, mapper).get("first_name"));
        assertSame(HashEntityOperations.cachedSnakeCaseMapper(mapper), HashEntityOperations.cachedSnakeCaseMapper(mapper));
    }

    static class SampleEntity {

        private String firstName;

        public String getFirstName() {
            return firstName;
        }

        public void setFirstName(String firstName) {
            this.firstName = firstName;
        }
    }
}
