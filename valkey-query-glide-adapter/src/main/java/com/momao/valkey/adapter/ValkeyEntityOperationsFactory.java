package com.momao.valkey.adapter;

import com.momao.valkey.annotation.StorageType;

final class ValkeyEntityOperationsFactory {

    private ValkeyEntityOperationsFactory() {
    }

    static <T> ValkeyEntityOperations<T> create(StorageType storageType) {
        return switch (storageType) {
            case JSON -> new JsonEntityOperations<>();
            case HASH -> new HashEntityOperations<>();
        };
    }
}
