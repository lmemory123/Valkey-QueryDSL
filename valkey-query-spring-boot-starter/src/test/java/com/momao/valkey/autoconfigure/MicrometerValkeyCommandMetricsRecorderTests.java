package com.momao.valkey.autoconfigure;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MicrometerValkeyCommandMetricsRecorderTests {

    @Test
    void recordsFailureCounterWithErrorCodeTags() {
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        try {
            MicrometerValkeyCommandMetricsRecorder recorder = new MicrometerValkeyCommandMetricsRecorder(meterRegistry);

            recorder.recordFailure("FT.SEARCH", "idx:sku", "QUERY_004", "QUERY", "read");
            recorder.recordSlowQuery("FT.SEARCH", "idx:sku", "read_all");

            assertEquals(1.0d, meterRegistry.get("valkey.query.command.failed")
                    .tag("db.system", "valkey")
                    .tag("db.operation", "FT.SEARCH")
                    .tag("db.namespace", "idx:sku")
                    .tag("valkey.route.type", "read")
                    .tag("valkey.error.code", "QUERY_004")
                    .tag("valkey.error.category", "QUERY")
                    .counter()
                    .count());
            assertEquals(1.0d, meterRegistry.get("valkey.query.command.slow.total")
                    .tag("db.system", "valkey")
                    .tag("db.operation", "FT.SEARCH")
                    .tag("db.namespace", "idx:sku")
                    .tag("valkey.route.type", "read_all")
                    .counter()
                    .count());
        } finally {
            meterRegistry.close();
        }
    }
}
