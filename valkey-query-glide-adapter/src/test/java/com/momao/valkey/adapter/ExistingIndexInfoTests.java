package com.momao.valkey.adapter;

import com.momao.valkey.annotation.StorageType;
import com.momao.valkey.core.metadata.IndexSchema;
import com.momao.valkey.core.metadata.SchemaField;
import glide.api.models.commands.FT.FTCreateOptions;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ExistingIndexInfoTests {

    @Test
    void matchesJsonSchemaWhenDefinitionIsConsistent() {
        IndexSchema schema = new IndexSchema(
                "idx:sku",
                StorageType.JSON,
                List.of("sku:"),
                List.of(
                        SchemaField.tag("id", ",", true),
                        SchemaField.text("title", 2.5d, true, false),
                        SchemaField.numeric("price", true),
                        SchemaField.tag("tags", "tags[*]", ",", false),
                        SchemaField.tag("merchant_name", "merchant.name", ",", false)
                ));

        Object[] response = new Object[]{
                "index_name", "idx:sku",
                "index_definition", new Object[]{
                "key_type", "JSON",
                "prefixes", new Object[]{"sku:"}
        },
                "attributes", new Object[]{
                new Object[]{"identifier", "$.id", "attribute", "id", "type", "TAG", "SORTABLE"},
                new Object[]{"identifier", "$.title", "attribute", "title", "type", "TEXT", "WEIGHT", "2.5", "NOSTEM"},
                new Object[]{"identifier", "$.price", "attribute", "price", "type", "NUMERIC", "SORTABLE"},
                new Object[]{"identifier", "$.tags[*]", "attribute", "tags", "type", "TAG", "SEPARATOR", ","},
                new Object[]{"identifier", "$.merchant.name", "attribute", "merchant_name", "type", "TAG", "SEPARATOR", ","}
        }
        };

        ExistingIndexInfo indexInfo = ExistingIndexInfo.fromResponse(response);

        assertTrue(indexInfo.matches(schema, FTCreateOptions.DataType.JSON));
    }

    @Test
    void detectsMismatchedStorageType() {
        IndexSchema schema = new IndexSchema(
                "idx:sku",
                StorageType.JSON,
                List.of("sku:"),
                List.of(SchemaField.tag("id", ",", true)));

        Object[] response = new Object[]{
                "index_definition", new Object[]{
                "key_type", "HASH",
                "prefixes", new Object[]{"sku:"}
        },
                "attributes", new Object[]{
                new Object[]{"identifier", "id", "attribute", "id", "type", "TAG", "SORTABLE"}
        }
        };

        ExistingIndexInfo indexInfo = ExistingIndexInfo.fromResponse(response);

        assertFalse(indexInfo.matches(schema, FTCreateOptions.DataType.JSON));
    }

    @Test
    void detectsMismatchedPrefixesOrFields() {
        IndexSchema schema = new IndexSchema(
                "idx:sku",
                StorageType.JSON,
                List.of("sku:"),
                List.of(SchemaField.tag("id", ",", true)));

        Object[] response = new Object[]{
                "index_definition", new Object[]{
                "key_type", "JSON",
                "prefixes", new Object[]{"legacy:"}
        },
                "attributes", new Object[]{
                new Object[]{"identifier", "$.legacyId", "attribute", "id", "type", "TAG", "SORTABLE"}
        }
        };

        ExistingIndexInfo indexInfo = ExistingIndexInfo.fromResponse(response);

        assertFalse(indexInfo.matches(schema, FTCreateOptions.DataType.JSON));
    }

    @Test
    void matchesJsonSchemaWhenAliasDiffersFromActualPropertyName() {
        IndexSchema schema = new IndexSchema(
                "idx:media_asset",
                StorageType.JSON,
                List.of("media:"),
                List.of(
                        SchemaField.tag("producer_mark", "producerMark", ",", false),
                        SchemaField.tag("audit_status", "auditStatus", ",", false),
                        SchemaField.tag("is_public", "isPublic", ",", false),
                        SchemaField.numeric("play_count", "playCount", true)
                ));

        Object[] response = new Object[]{
                "index_definition", new Object[]{
                "key_type", "JSON",
                "prefixes", new Object[]{"media:"}
        },
                "attributes", new Object[]{
                new Object[]{"identifier", "$.producerMark", "attribute", "producer_mark", "type", "TAG", "SEPARATOR", ","},
                new Object[]{"identifier", "$.auditStatus", "attribute", "audit_status", "type", "TAG", "SEPARATOR", ","},
                new Object[]{"identifier", "$.isPublic", "attribute", "is_public", "type", "TAG", "SEPARATOR", ","},
                new Object[]{"identifier", "$.playCount", "attribute", "play_count", "type", "NUMERIC", "SORTABLE"}
        }
        };

        ExistingIndexInfo indexInfo = ExistingIndexInfo.fromResponse(response);

        assertTrue(indexInfo.matches(schema, FTCreateOptions.DataType.JSON));
    }

    @Test
    void detectsTextWeightMismatch() {
        IndexSchema schema = new IndexSchema(
                "idx:sku",
                StorageType.JSON,
                List.of("sku:"),
                List.of(SchemaField.text("title", 2.5d, true, false))
        );

        Object[] response = new Object[]{
                "index_definition", new Object[]{
                "key_type", "JSON",
                "prefixes", new Object[]{"sku:"}
        },
                "attributes", new Object[]{
                new Object[]{"identifier", "$.title", "attribute", "title", "type", "TEXT", "WEIGHT", "1.0", "NOSTEM"}
        }
        };

        ExistingIndexInfo indexInfo = ExistingIndexInfo.fromResponse(response);

        assertFalse(indexInfo.matches(schema, FTCreateOptions.DataType.JSON));
        assertFalse(indexInfo.diff(schema, FTCreateOptions.DataType.JSON).isEmpty());
    }
}
