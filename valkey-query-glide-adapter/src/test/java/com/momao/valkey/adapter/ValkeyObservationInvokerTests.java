package com.momao.valkey.adapter;

import com.momao.valkey.adapter.observability.SlowLogEntry;
import com.momao.valkey.adapter.observability.SlowLogRecorder;
import com.momao.valkey.adapter.observability.ValkeyCommandMetricsRecorder;
import com.momao.valkey.adapter.observability.ValkeyObservationInvoker;
import com.momao.valkey.core.exception.ValkeyErrorCode;
import com.momao.valkey.core.exception.ValkeyQueryExecutionException;
import io.micrometer.common.KeyValues;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationHandler;
import io.micrometer.observation.ObservationRegistry;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ValkeyObservationInvokerTests {

    @Test
    void bareBranchDoesNotEvaluateQuerySupplier() throws Exception {
        AtomicInteger queryCalls = new AtomicInteger();
        ValkeyObservationInvoker invoker = new ValkeyObservationInvoker(
                ObservationRegistry.NOOP,
                false,
                false,
                false,
                200L,
                SlowLogRecorder.NOOP,
                ValkeyCommandMetricsRecorder.NOOP
        );

        String result = invoker.execute("FT.SEARCH", "idx:test", () -> {
            queryCalls.incrementAndGet();
            return "@title:test";
        }, () -> "OK");

        assertEquals("OK", result);
        assertEquals(0, queryCalls.get());
    }

    @Test
    void slowLogBranchEvaluatesQuerySupplierOnlyWhenThresholdIsHit() throws Exception {
        AtomicInteger queryCalls = new AtomicInteger();
        List<SlowLogEntry> slowLogs = new CopyOnWriteArrayList<>();
        ValkeyObservationInvoker invoker = new ValkeyObservationInvoker(
                ObservationRegistry.NOOP,
                false,
                true,
                false,
                1L,
                slowLogs::add,
                ValkeyCommandMetricsRecorder.NOOP
        );

        invoker.execute("FT.SEARCH", "idx:test", () -> {
            queryCalls.incrementAndGet();
            return "@title:test";
        }, () -> {
            Thread.sleep(5L);
            return "OK";
        });

        assertEquals(1, queryCalls.get());
        assertEquals(1, slowLogs.size());
        assertEquals("@title:test", slowLogs.get(0).statement());
        assertEquals("unknown", slowLogs.get(0).routeType());
    }

    @Test
    void observationBranchDoesNotAttachHighCardinalityQueryOnFastSuccess() throws Exception {
        ObservationRegistry registry = ObservationRegistry.create();
        AtomicReference<KeyValues> lowCardinality = new AtomicReference<>(KeyValues.empty());
        AtomicReference<KeyValues> highCardinality = new AtomicReference<>(KeyValues.empty());
        registry.observationConfig().observationHandler(new CaptureHandler(lowCardinality, highCardinality));
        AtomicInteger queryCalls = new AtomicInteger();
        ValkeyObservationInvoker invoker = new ValkeyObservationInvoker(
                registry,
                true,
                false,
                false,
                200L,
                SlowLogRecorder.NOOP,
                ValkeyCommandMetricsRecorder.NOOP
        );

        invoker.execute("FT.SEARCH", "idx:test", () -> {
            queryCalls.incrementAndGet();
            return "@title:test";
        }, () -> "OK");

        assertEquals(0, queryCalls.get());
        assertTrue(lowCardinality.get().stream().anyMatch(keyValue ->
                "db.system".equals(keyValue.getKey()) && "valkey".equals(keyValue.getValue())));
        assertTrue(lowCardinality.get().stream().anyMatch(keyValue ->
                "db.operation".equals(keyValue.getKey()) && "FT.SEARCH".equals(keyValue.getValue())));
        assertTrue(lowCardinality.get().stream().anyMatch(keyValue ->
                "db.namespace".equals(keyValue.getKey()) && "idx:test".equals(keyValue.getValue())));
        assertTrue(lowCardinality.get().stream().anyMatch(keyValue ->
                "valkey.route.type".equals(keyValue.getKey()) && "unknown".equals(keyValue.getValue())));
        assertTrue(highCardinality.get().stream().findAny().isEmpty());
    }

    @Test
    void observationBranchAttachesHighCardinalityQueryOnError() {
        ObservationRegistry registry = ObservationRegistry.create();
        AtomicReference<KeyValues> lowCardinality = new AtomicReference<>(KeyValues.empty());
        AtomicReference<KeyValues> highCardinality = new AtomicReference<>(KeyValues.empty());
        registry.observationConfig().observationHandler(new CaptureHandler(lowCardinality, highCardinality));
        AtomicInteger queryCalls = new AtomicInteger();
        ValkeyObservationInvoker invoker = new ValkeyObservationInvoker(
                registry,
                true,
                false,
                false,
                200L,
                SlowLogRecorder.NOOP,
                ValkeyCommandMetricsRecorder.NOOP
        );

        ValkeyQueryExecutionException exception = assertThrows(ValkeyQueryExecutionException.class, () -> invoker.execute(
                "FT.SEARCH",
                "idx:test",
                () -> {
                    queryCalls.incrementAndGet();
                    return "@title:test";
                },
                () -> {
                    throw new ValkeyQueryExecutionException(ValkeyErrorCode.QUERY_SEARCH_FAILED, "boom");
                }
        ));

        assertEquals("boom", exception.getMessage());
        assertEquals(1, queryCalls.get());
        assertTrue(lowCardinality.get().stream().anyMatch(keyValue ->
                "db.system".equals(keyValue.getKey()) && "valkey".equals(keyValue.getValue())));
        assertTrue(lowCardinality.get().stream().anyMatch(keyValue ->
                "valkey.error.code".equals(keyValue.getKey()) && "QUERY_004".equals(keyValue.getValue())));
        assertTrue(lowCardinality.get().stream().anyMatch(keyValue ->
                "valkey.error.category".equals(keyValue.getKey()) && "QUERY".equals(keyValue.getValue())));
        assertTrue(highCardinality.get().stream().anyMatch(keyValue ->
                "db.statement".equals(keyValue.getKey()) && "@title:test".equals(keyValue.getValue())));
    }

    @Test
    void observationBranchAttachesStatementWhenTraceQueryTextEnabled() throws Exception {
        ObservationRegistry registry = ObservationRegistry.create();
        AtomicReference<KeyValues> lowCardinality = new AtomicReference<>(KeyValues.empty());
        AtomicReference<KeyValues> highCardinality = new AtomicReference<>(KeyValues.empty());
        registry.observationConfig().observationHandler(new CaptureHandler(lowCardinality, highCardinality));
        ValkeyObservationInvoker invoker = new ValkeyObservationInvoker(
                registry,
                true,
                false,
                true,
                200L,
                SlowLogRecorder.NOOP,
                ValkeyCommandMetricsRecorder.NOOP
        );

        invoker.execute("FT.SEARCH", "idx:test", () -> "@title:test", () -> "OK");

        assertTrue(lowCardinality.get().stream().anyMatch(keyValue ->
                "db.operation".equals(keyValue.getKey()) && "FT.SEARCH".equals(keyValue.getValue())));
        assertTrue(highCardinality.get().stream().anyMatch(keyValue ->
                "db.statement".equals(keyValue.getKey()) && "@title:test".equals(keyValue.getValue())));
    }

    @Test
    void metricsOnlyBranchRecordsFailureByErrorCode() {
        AtomicReference<String> metricLine = new AtomicReference<>();
        ValkeyObservationInvoker invoker = new ValkeyObservationInvoker(
                ObservationRegistry.NOOP,
                false,
                false,
                false,
                0L,
                SlowLogRecorder.NOOP,
                (commandName, indexName, errorCode, errorCategory) ->
                        metricLine.set(commandName + "|" + indexName + "|" + errorCode + "|" + errorCategory)
        );

        ValkeyQueryExecutionException exception = assertThrows(ValkeyQueryExecutionException.class, () -> invoker.execute(
                "FT.SEARCH",
                "idx:test",
                () -> "@title:test",
                () -> {
                    throw new ValkeyQueryExecutionException(ValkeyErrorCode.QUERY_SEARCH_FAILED, "boom");
                }
        ));

        assertEquals("boom", exception.getMessage());
        assertEquals("FT.SEARCH|idx:test|QUERY_004|QUERY", metricLine.get());
    }

    @Test
    void explicitRouteTypeIsAddedToObservationAndSlowLog() throws Exception {
        ObservationRegistry registry = ObservationRegistry.create();
        AtomicReference<KeyValues> lowCardinality = new AtomicReference<>(KeyValues.empty());
        AtomicReference<KeyValues> highCardinality = new AtomicReference<>(KeyValues.empty());
        registry.observationConfig().observationHandler(new CaptureHandler(lowCardinality, highCardinality));
        List<SlowLogEntry> slowLogs = new CopyOnWriteArrayList<>();
        ValkeyObservationInvoker invoker = new ValkeyObservationInvoker(
                registry,
                true,
                true,
                false,
                1L,
                slowLogs::add,
                ValkeyCommandMetricsRecorder.NOOP
        );

        invoker.execute("FT.SEARCH", "idx:test", () -> "@title:test", "read_all", () -> {
            Thread.sleep(5L);
            return "OK";
        });

        assertTrue(lowCardinality.get().stream().anyMatch(keyValue ->
                "valkey.route.type".equals(keyValue.getKey()) && "read_all".equals(keyValue.getValue())));
        assertEquals(1, slowLogs.size());
        assertEquals("read_all", slowLogs.get(0).routeType());
        assertTrue(highCardinality.get().stream().findAny().isEmpty());
    }

    private static final class CaptureHandler implements ObservationHandler<Observation.Context> {

        private final AtomicReference<KeyValues> lowCardinality;
        private final AtomicReference<KeyValues> highCardinality;

        private CaptureHandler(AtomicReference<KeyValues> lowCardinality, AtomicReference<KeyValues> highCardinality) {
            this.lowCardinality = lowCardinality;
            this.highCardinality = highCardinality;
        }

        @Override
        public void onStop(Observation.Context context) {
            lowCardinality.set(context.getLowCardinalityKeyValues());
            highCardinality.set(context.getHighCardinalityKeyValues());
        }

        @Override
        public boolean supportsContext(Observation.Context context) {
            return true;
        }
    }
}
