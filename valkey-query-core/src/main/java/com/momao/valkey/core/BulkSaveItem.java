package com.momao.valkey.core;

public record BulkSaveItem<T>(String id, T entity) {

    public BulkSaveItem {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("Bulk save item id cannot be blank");
        }
        if (entity == null) {
            throw new IllegalArgumentException("Bulk save item entity cannot be null");
        }
    }

    public static <T> BulkSaveItem<T> of(String id, T entity) {
        return new BulkSaveItem<>(id, entity);
    }
}
