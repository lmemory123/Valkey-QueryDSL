package com.momao.valkey.autoconfigure;

import com.momao.valkey.adapter.IndexDiff;
import com.momao.valkey.adapter.IndexDiffItem;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;

final class ValkeyIndexMetrics {

    private static final ValkeyIndexMetrics NOOP = new ValkeyIndexMetrics(null);

    private final MeterRegistry meterRegistry;

    private ValkeyIndexMetrics(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    static ValkeyIndexMetrics noop() {
        return NOOP;
    }

    static ValkeyIndexMetrics of(MeterRegistry meterRegistry) {
        return meterRegistry == null ? NOOP : new ValkeyIndexMetrics(meterRegistry);
    }

    void recordDiff(IndexDiff diff, IndexManagementMode mode) {
        if (meterRegistry == null || diff == null || diff.isEmpty()) {
            return;
        }
        for (IndexDiffItem item : diff.items()) {
            Counter.builder("valkey.query.index.diff")
                    .description("Detected Valkey index diffs grouped by mode and diff type")
                    .tag("db.system", "valkey")
                    .tag("db.namespace", normalize(diff.indexName()))
                    .tag("index.management.mode", normalize(mode == null ? null : mode.name().toLowerCase()))
                    .tag("index.diff.type", normalize(item.type() == null ? null : item.type().name().toLowerCase()))
                    .register(meterRegistry)
                    .increment();
        }
    }

    void recordValidationFailure(String indexName, String errorCode, String errorCategory) {
        if (meterRegistry == null) {
            return;
        }
        Counter.builder("valkey.query.index.validation.failed")
                .description("Valkey index validation failures grouped by error code")
                .tag("db.system", "valkey")
                .tag("db.namespace", normalize(indexName))
                .tag("valkey.error.code", normalize(errorCode))
                .tag("valkey.error.category", normalize(errorCategory))
                .register(meterRegistry)
                .increment();
    }

    void recordRecreate(String indexName) {
        if (meterRegistry == null) {
            return;
        }
        Counter.builder("valkey.query.index.recreate")
                .description("Valkey index recreate operations triggered by index management")
                .tag("db.system", "valkey")
                .tag("db.namespace", normalize(indexName))
                .register(meterRegistry)
                .increment();
    }

    private String normalize(String value) {
        return value == null || value.isBlank() ? "unknown" : value;
    }
}
