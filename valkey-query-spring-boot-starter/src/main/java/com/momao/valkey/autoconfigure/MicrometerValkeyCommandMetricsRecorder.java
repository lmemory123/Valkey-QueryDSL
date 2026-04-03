package com.momao.valkey.autoconfigure;

import com.momao.valkey.adapter.observability.ValkeyCommandMetricsRecorder;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

final class MicrometerValkeyCommandMetricsRecorder implements ValkeyCommandMetricsRecorder {

    private final MeterRegistry meterRegistry;

    MicrometerValkeyCommandMetricsRecorder(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    @Override
    public void recordFailure(String commandName, String indexName, String errorCode, String errorCategory) {
        recordFailure(commandName, indexName, errorCode, errorCategory, "unknown");
    }

    @Override
    public void recordFailure(String commandName, String indexName, String errorCode, String errorCategory, String routeType) {
        Counter.builder("valkey.query.command.failed")
                .description("Failed Valkey commands grouped by command and error code")
                .tag("db.system", "valkey")
                .tag("db.operation", normalize(commandName))
                .tag("db.namespace", normalize(indexName))
                .tag("valkey.route.type", normalize(routeType))
                .tag("valkey.error.code", normalize(errorCode))
                .tag("valkey.error.category", normalize(errorCategory))
                .register(meterRegistry)
                .increment();
    }

    @Override
    public void recordSlowQuery(String commandName, String indexName, String routeType) {
        Counter.builder("valkey.query.command.slow.total")
                .description("Slow Valkey commands grouped by command and route type")
                .tag("db.system", "valkey")
                .tag("db.operation", normalize(commandName))
                .tag("db.namespace", normalize(indexName))
                .tag("valkey.route.type", normalize(routeType))
                .register(meterRegistry)
                .increment();
    }

    private String normalize(String value) {
        return value == null || value.isBlank() ? "unknown" : value;
    }
}
