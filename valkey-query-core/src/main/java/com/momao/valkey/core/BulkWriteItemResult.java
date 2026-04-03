package com.momao.valkey.core;

public record BulkWriteItemResult(
        String operation,
        String id,
        boolean success,
        String errorCode,
        String message
) {

    public static BulkWriteItemResult success(String operation, String id) {
        return new BulkWriteItemResult(operation, id, true, null, null);
    }

    public static BulkWriteItemResult failure(String operation, String id, String errorCode, String message) {
        return new BulkWriteItemResult(operation, id, false, errorCode, message);
    }
}
