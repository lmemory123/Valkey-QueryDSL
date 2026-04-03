package com.momao.valkey.autoconfigure;

import com.momao.valkey.adapter.observability.ValkeyUpdateMetricsRecorder;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

final class MicrometerValkeyUpdateMetricsRecorder implements ValkeyUpdateMetricsRecorder {

    private final MeterRegistry meterRegistry;

    MicrometerValkeyUpdateMetricsRecorder(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    @Override
    public void recordPartialUpdate(String indexName, String outcome) {
        recordPartialUpdate(indexName, outcome, "unknown");
    }

    @Override
    public void recordPartialUpdate(String indexName, String outcome, String updateKind) {
        Counter.builder("valkey.query.update.partial.total")
                .description("Partial update attempts grouped by result")
                .tag("db.system", "valkey")
                .tag("db.namespace", normalize(indexName))
                .tag("valkey.update.outcome", normalize(outcome))
                .tag("valkey.update.kind", normalize(updateKind))
                .register(meterRegistry)
                .increment();
    }

    @Override
    public void recordPartialUpdateFailure(String indexName, String errorCode, String errorCategory) {
        recordPartialUpdateFailure(indexName, errorCode, errorCategory, "unknown");
    }

    @Override
    public void recordPartialUpdateFailure(String indexName, String errorCode, String errorCategory, String updateKind) {
        Counter.builder("valkey.query.update.partial.failed")
                .description("Failed partial updates grouped by error code")
                .tag("db.system", "valkey")
                .tag("db.namespace", normalize(indexName))
                .tag("valkey.update.kind", normalize(updateKind))
                .tag("valkey.error.code", normalize(errorCode))
                .tag("valkey.error.category", normalize(errorCategory))
                .register(meterRegistry)
                .increment();
    }

    private String normalize(String value) {
        return value == null || value.isBlank() ? "unknown" : value;
    }
}
