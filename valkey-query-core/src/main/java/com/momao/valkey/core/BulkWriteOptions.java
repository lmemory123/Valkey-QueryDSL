package com.momao.valkey.core;

public record BulkWriteOptions(
        BulkMode mode,
        int batchSize,
        boolean collectItemResults
) {

    private static final int DEFAULT_BATCH_SIZE = 100;

    public BulkWriteOptions {
        mode = mode == null ? BulkMode.ORDERED : mode;
        batchSize = batchSize <= 0 ? DEFAULT_BATCH_SIZE : batchSize;
    }

    public static BulkWriteOptions defaults() {
        return new BulkWriteOptions(BulkMode.ORDERED, DEFAULT_BATCH_SIZE, true);
    }

    public static BulkWriteOptions ordered() {
        return defaults();
    }

    public static BulkWriteOptions unordered() {
        return new BulkWriteOptions(BulkMode.UNORDERED, DEFAULT_BATCH_SIZE, true);
    }
}
