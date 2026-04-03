package com.momao.valkey.example;

import com.momao.valkey.annotation.FieldType;
import com.momao.valkey.annotation.StorageType;
import com.momao.valkey.core.VectorFieldBuilder;
import com.momao.valkey.core.metadata.SchemaField;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class QVectorItemTests {

    @Test
    void metadataIncludesVectorField() {
        SchemaField field = VectorItemQuery.METADATA.fields().stream()
                .filter(candidate -> candidate.fieldName().equals("embedding"))
                .findFirst()
                .orElseThrow();

        assertEquals("idx:vector_item", VectorItemQuery.METADATA.indexName());
        assertEquals(StorageType.JSON, VectorItemQuery.METADATA.storageType());
        assertEquals(FieldType.VECTOR, field.type());
        assertEquals(3, field.vectorOptions().dimension());
    }

    @Test
    void generatedQueryUsesVectorFieldBuilder() {
        assertInstanceOf(VectorFieldBuilder.class, new VectorItemQuery().embedding);
    }
}
