package com.momao.valkey.adapter.observability;

public interface ValkeyUpdateMetricsRecorder {

    ValkeyUpdateMetricsRecorder NOOP = new ValkeyUpdateMetricsRecorder() { };

    default void recordPartialUpdate(String indexName, String outcome) {
    }

    default void recordPartialUpdate(String indexName, String outcome, String updateKind) {
        recordPartialUpdate(indexName, outcome);
    }

    default void recordPartialUpdateFailure(String indexName, String errorCode, String errorCategory) {
    }

    default void recordPartialUpdateFailure(String indexName, String errorCode, String errorCategory, String updateKind) {
        recordPartialUpdateFailure(indexName, errorCode, errorCategory);
    }
}
