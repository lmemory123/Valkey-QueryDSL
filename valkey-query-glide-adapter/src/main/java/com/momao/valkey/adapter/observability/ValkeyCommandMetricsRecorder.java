package com.momao.valkey.adapter.observability;

public interface ValkeyCommandMetricsRecorder {

    ValkeyCommandMetricsRecorder NOOP = (commandName, indexName, errorCode, errorCategory) -> { };

    void recordFailure(String commandName, String indexName, String errorCode, String errorCategory);

    default void recordFailure(String commandName, String indexName, String errorCode, String errorCategory, String routeType) {
        recordFailure(commandName, indexName, errorCode, errorCategory);
    }

    default void recordSlowQuery(String commandName, String indexName, String routeType) {
    }
}
