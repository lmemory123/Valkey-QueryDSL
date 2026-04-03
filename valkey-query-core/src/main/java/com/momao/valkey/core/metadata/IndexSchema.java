package com.momao.valkey.core.metadata;

import com.momao.valkey.annotation.StorageType;

import java.util.List;

public record IndexSchema(
    String indexName,
    String aliasName,
    StorageType storageType,
    List<String> prefixes,
    List<SchemaField> fields
) {

    public IndexSchema {
        aliasName = aliasName == null || aliasName.isBlank() ? indexName : aliasName;
        prefixes = prefixes == null ? List.of() : List.copyOf(prefixes);
        fields = fields == null ? List.of() : List.copyOf(fields);
    }

    public IndexSchema(String indexName, StorageType storageType, List<String> prefixes, List<SchemaField> fields) {
        this(indexName, indexName, storageType, prefixes, fields);
    }

    public IndexSchema(String indexName, String prefix, List<SchemaField> fields) {
        this(indexName, indexName, StorageType.HASH, prefix == null || prefix.isEmpty() ? List.of() : List.of(prefix), fields);
    }

    public String prefix() {
        return prefixes.isEmpty() ? "" : prefixes.get(0);
    }

    public String queryTargetName() {
        return aliasName;
    }

    public boolean usesAlias() {
        return aliasName != null && !aliasName.equals(indexName);
    }
}
