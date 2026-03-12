package com.momao.valkey.example;

import com.momao.valkey.annotation.FieldType;
import com.momao.valkey.annotation.StorageType;
import com.momao.valkey.core.SearchCondition;
import com.momao.valkey.core.metadata.SchemaField;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class QMediaAssetTests {

    private static final MediaAssetQuery qMediaAsset = new MediaAssetQuery();

    @Test
    void jsonAliasBuildsQueryWithAliasName() {
        SearchCondition condition = qMediaAsset.producerMark.eq("sony");

        assertEquals("@producer_mark:{sony}", condition.build());
    }

    @Test
    void metadataSeparatesJsonPathFromAlias() {
        List<SchemaField> fields = MediaAssetQuery.METADATA.fields();

        assertEquals("idx:media_asset", MediaAssetQuery.METADATA.indexName());
        assertEquals(StorageType.JSON, MediaAssetQuery.METADATA.storageType());
        assertEquals(List.of("media:"), MediaAssetQuery.METADATA.prefixes());
        assertEquals(List.of(
                new SchemaField("id", "id", FieldType.TAG, true, 1.0d, false, ","),
                new SchemaField("producer_mark", "producerMark", FieldType.TAG, false, 1.0d, false, ","),
                new SchemaField("audit_status", "auditStatus", FieldType.TAG, false, 1.0d, false, ","),
                new SchemaField("is_public", "isPublic", FieldType.TAG, false, 1.0d, false, ","),
                new SchemaField("play_count", "playCount", FieldType.NUMERIC, true, 1.0d, false, ",")
        ), fields);
    }
}