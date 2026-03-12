package com.momao.valkey.core.metadata;

import com.momao.valkey.annotation.StorageType;

import java.util.List;

public record IndexSchema(
    String indexName,
    StorageType storageType,
    List<String> prefixes,
    List<SchemaField> fields
) {

    public IndexSchema {
        prefixes = prefixes == null ? List.of() : List.copyOf(prefixes);
        fields = fields == null ? List.of() : List.copyOf(fields);
    }

    public IndexSchema(String indexName, String prefix, List<SchemaField> fields) {
        this(indexName, StorageType.HASH, prefix == null || prefix.isEmpty() ? List.of() : List.of(prefix), fields);
    }

    public String prefix() {
        return prefixes.isEmpty() ? "" : prefixes.get(0);
    }
}
