package com.momao.valkey.adapter;

import com.momao.valkey.annotation.StorageType;
import com.momao.valkey.core.metadata.IndexSchema;

final class ValkeyEntityOperationsFactory {

    private ValkeyEntityOperationsFactory() {
    }

    static <T> ValkeyEntityOperations<T> create(StorageType storageType, IndexSchema schema) {
        return switch (storageType) {
            case JSON -> new JsonEntityOperations<>(schema);
            case HASH -> new HashEntityOperations<>(schema);
        };
    }
}
