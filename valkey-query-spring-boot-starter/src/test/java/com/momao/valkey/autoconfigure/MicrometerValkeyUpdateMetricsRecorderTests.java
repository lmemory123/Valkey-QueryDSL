package com.momao.valkey.autoconfigure;

import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class MicrometerValkeyUpdateMetricsRecorderTests {

    @Test
    void recordsPartialUpdateCounters() {
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        try {
            MicrometerValkeyUpdateMetricsRecorder recorder = new MicrometerValkeyUpdateMetricsRecorder(meterRegistry);

            recorder.recordPartialUpdate("idx:student", "updated", "increment");
            recorder.recordPartialUpdateFailure("idx:student", "QUERY_003", "QUERY", "mixed");

            assertEquals(1.0d, meterRegistry.get("valkey.query.update.partial.total")
                    .tag("db.system", "valkey")
                    .tag("db.namespace", "idx:student")
                    .tag("valkey.update.outcome", "updated")
                    .tag("valkey.update.kind", "increment")
                    .counter()
                    .count());
            assertEquals(1.0d, meterRegistry.get("valkey.query.update.partial.failed")
                    .tag("db.system", "valkey")
                    .tag("db.namespace", "idx:student")
                    .tag("valkey.update.kind", "mixed")
                    .tag("valkey.error.code", "QUERY_003")
                    .tag("valkey.error.category", "QUERY")
                    .counter()
                    .count());
        } finally {
            meterRegistry.close();
        }
    }
}
