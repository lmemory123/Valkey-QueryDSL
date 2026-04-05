package com.momao.valkey.example;

import com.momao.valkey.annotation.FieldType;
import com.momao.valkey.annotation.StorageType;
import com.momao.valkey.core.SearchCondition;
import com.momao.valkey.core.metadata.SchemaField;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class QSkuTests {

    private static final SkuQuery qSku = new SkuQuery();

    @Test
    void nestedMerchantFieldBuildsCondition() {
        SearchCondition condition = qSku.merchant.name.eq("Apple");

        assertEquals("@merchant_name:{Apple}", condition.build());
    }

    @Test
    void collectionFieldSupportsContainsSyntax() {
        SearchCondition condition = qSku.tags.contains("HOT");

        assertEquals("@tags:{HOT}", condition.build());
    }

    @Test
    void metadataUsesConventionDefaultsAndNestedJsonPath() {
        List<SchemaField> fields = SkuQuery.METADATA.fields();

        assertEquals("idx:sku", SkuQuery.METADATA.indexName());
        assertEquals(StorageType.JSON, SkuQuery.METADATA.storageType());
        assertEquals(List.of("sku:"), SkuQuery.METADATA.prefixes());
        assertEquals(List.of(
                new SchemaField("id", "id", FieldType.TAG, false, 1.0d, false, ","),
                new SchemaField("title", "title", FieldType.TEXT, false, 2.5d, true, ","),
                new SchemaField("price", "price", FieldType.NUMERIC, true, 1.0d, false, ","),
                new SchemaField("tags", "tags[*]", FieldType.TAG, false, 1.0d, false, ","),
                new SchemaField("merchant_name", "merchant.name", FieldType.TAG, false, 1.0d, false, ",")
        ), fields);
    }
}
