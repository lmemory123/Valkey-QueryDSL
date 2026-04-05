package com.momao.valkey.example;

import com.momao.valkey.annotation.FieldType;
import com.momao.valkey.annotation.StorageType;
import com.momao.valkey.core.metadata.SchemaField;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class QAnnotationCoverageTests {

    private static final AnnotationCoverageDocumentSearchQuery q = new AnnotationCoverageDocumentSearchQuery();

    @Test
    void metadataCoversAllAnnotationSettings() {
        assertEquals("idx:annotation_coverage", AnnotationCoverageDocumentSearchQuery.METADATA.indexName());
        assertEquals(StorageType.JSON, AnnotationCoverageDocumentSearchQuery.METADATA.storageType());
        assertEquals(List.of("annotation:"), AnnotationCoverageDocumentSearchQuery.METADATA.prefixes());

        List<SchemaField> fields = AnnotationCoverageDocumentSearchQuery.METADATA.fields();
        assertEquals(List.of(
                new SchemaField("id", "id", FieldType.TAG, false, 1.0d, false, ","),
                new SchemaField("title_text", "title", FieldType.TEXT, true, 3.0d, true, ","),
                new SchemaField("status_tag", "status", FieldType.TAG, false, 1.0d, false, ","),
                new SchemaField("play_count", "playCount", FieldType.NUMERIC, true, 1.0d, false, ","),
                new SchemaField("rank", "rank", FieldType.NUMERIC, true, 1.0d, false, ","),
                new SchemaField("keyword_text", "keyword", FieldType.TEXT, true, 1.0d, false, ","),
                new SchemaField("labels_tag", "labels[*]", FieldType.TAG, false, 1.0d, false, ","),
                SchemaField.vector("embedding_vector", "embedding", 4, com.momao.valkey.annotation.DistanceMetric.IP, 8, 32)
        ), fields);
    }

    @Test
    void generatedBuildersMatchExplicitAndInferredFieldTypes() {
        assertEquals("@status_tag:{PUBLIC}", q.status.eq("PUBLIC").build());
        assertEquals("@play_count:[10 10]", q.playCount.eq(10).build());
        assertEquals("@rank:[7 7]", q.rank.eq(7L).build());
        assertEquals("@keyword_text:hello", q.keyword.contains("hello").build());
        assertEquals("@labels_tag:{HOT}", q.labels.contains("HOT").build());
        assertTrue(q.embedding != null);
    }
}
