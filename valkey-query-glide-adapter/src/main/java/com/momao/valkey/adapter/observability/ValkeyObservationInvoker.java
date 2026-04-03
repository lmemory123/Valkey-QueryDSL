package com.momao.valkey.adapter.observability;

import com.momao.valkey.core.exception.ValkeyQueryException;
import io.micrometer.observation.Observation;
import io.micrometer.observation.ObservationRegistry;

import java.util.function.Supplier;

public final class ValkeyObservationInvoker {

    private static final ValkeyObservationInvoker NOOP = new ValkeyObservationInvoker(
            ObservationRegistry.NOOP,
            false,
            false,
            false,
            0L,
            SlowLogRecorder.NOOP,
            ValkeyCommandMetricsRecorder.NOOP
    );

    private final ObservationRegistry registry;
    private final boolean observabilityEnabled;
    private final boolean slowLogEnabled;
    private final boolean traceQueryTextEnabled;
    private final long slowQueryThresholdMs;
    private final SlowLogRecorder slowLogRecorder;
    private final ValkeyCommandMetricsRecorder commandMetricsRecorder;
    private final boolean commandMetricsEnabled;

    public ValkeyObservationInvoker(
            ObservationRegistry registry,
            boolean observabilityEnabled,
            boolean slowLogEnabled,
            boolean traceQueryTextEnabled,
            long slowQueryThresholdMs,
            SlowLogRecorder slowLogRecorder) {
        this(
                registry,
                observabilityEnabled,
                slowLogEnabled,
                traceQueryTextEnabled,
                slowQueryThresholdMs,
                slowLogRecorder,
                ValkeyCommandMetricsRecorder.NOOP
        );
    }

    public ValkeyObservationInvoker(
            ObservationRegistry registry,
            boolean observabilityEnabled,
            boolean slowLogEnabled,
            boolean traceQueryTextEnabled,
            long slowQueryThresholdMs,
            SlowLogRecorder slowLogRecorder,
            ValkeyCommandMetricsRecorder commandMetricsRecorder) {
        this.registry = registry == null ? ObservationRegistry.NOOP : registry;
        this.observabilityEnabled = observabilityEnabled && !this.registry.isNoop();
        this.slowLogEnabled = slowLogEnabled && slowQueryThresholdMs > 0;
        this.traceQueryTextEnabled = traceQueryTextEnabled;
        this.slowQueryThresholdMs = Math.max(0L, slowQueryThresholdMs);
        this.slowLogRecorder = slowLogRecorder == null ? SlowLogRecorder.NOOP : slowLogRecorder;
        this.commandMetricsRecorder = commandMetricsRecorder == null ? ValkeyCommandMetricsRecorder.NOOP : commandMetricsRecorder;
        this.commandMetricsEnabled = this.commandMetricsRecorder != ValkeyCommandMetricsRecorder.NOOP;
    }

    public static ValkeyObservationInvoker noop() {
        return NOOP;
    }

    public <T> T execute(
            String commandName,
            String indexName,
            Supplier<String> querySupplier,
            ValkeyCommandCallback<T> callback) throws Exception {
        return execute(commandName, indexName, querySupplier, "unknown", callback);
    }

    public <T> T execute(
            String commandName,
            String indexName,
            Supplier<String> querySupplier,
            String routeType,
            ValkeyCommandCallback<T> callback) throws Exception {
        boolean obsEnabled = observabilityEnabled;
        boolean slowEnabled = slowLogEnabled;
        boolean metricsEnabled = commandMetricsEnabled;
        if (!obsEnabled && !slowEnabled && !metricsEnabled) {
            return callback.execute();
        }
        if (!obsEnabled && !metricsEnabled) {
            return executeWithSlowLogOnly(commandName, indexName, querySupplier, routeType, callback);
        }
        if (!obsEnabled) {
            return executeWithFailureMetrics(commandName, indexName, querySupplier, routeType, callback, slowEnabled);
        }
        return executeWithObservation(commandName, indexName, querySupplier, routeType, callback, slowEnabled);
    }

    private <T> T executeWithSlowLogOnly(
            String commandName,
            String indexName,
            Supplier<String> querySupplier,
            String routeType,
            ValkeyCommandCallback<T> callback) throws Exception {
        long startNanos = System.nanoTime();
        try {
            T result = callback.execute();
            recordSlowQueryIfNeeded(commandName, indexName, querySupplier, routeType, startNanos);
            return result;
        } catch (Exception exception) {
            recordSlowQueryIfNeeded(commandName, indexName, querySupplier, routeType, startNanos);
            throw exception;
        }
    }

    private <T> T executeWithFailureMetrics(
            String commandName,
            String indexName,
            Supplier<String> querySupplier,
            String routeType,
            ValkeyCommandCallback<T> callback,
            boolean slowEnabled) throws Exception {
        long startNanos = slowEnabled ? System.nanoTime() : 0L;
        try {
            T result = callback.execute();
            if (slowEnabled) {
                recordSlowQueryIfNeeded(commandName, indexName, querySupplier, routeType, startNanos);
            }
            return result;
        } catch (Exception exception) {
            recordFailureMetric(commandName, indexName, routeType, exception);
            if (slowEnabled) {
                recordSlowQueryIfNeeded(commandName, indexName, querySupplier, routeType, startNanos);
            }
            throw exception;
        }
    }

    private <T> T executeWithObservation(
            String commandName,
            String indexName,
            Supplier<String> querySupplier,
            String routeType,
            ValkeyCommandCallback<T> callback,
            boolean slowEnabled) throws Exception {
        Observation observation = Observation.createNotStarted("valkey.command", registry)
                .contextualName("Valkey " + commandName)
                .lowCardinalityKeyValue("db.system", "valkey")
                .lowCardinalityKeyValue("db.operation", normalize(commandName))
                .lowCardinalityKeyValue("db.namespace", normalize(indexName))
                .lowCardinalityKeyValue("valkey.route.type", normalize(routeType))
                .start();
        long startNanos = System.nanoTime();
        Exception error = null;
        try (Observation.Scope ignored = observation.openScope()) {
            return callback.execute();
        } catch (Exception exception) {
            error = exception;
            observation.error(exception);
            throw exception;
        } finally {
            long elapsedMs = elapsedMillis(startNanos);
            boolean slow = slowEnabled && elapsedMs >= slowQueryThresholdMs;
            observation.lowCardinalityKeyValue("valkey.outcome", error == null ? "success" : "error");
            observation.lowCardinalityKeyValue("valkey.slow", Boolean.toString(slow));
            if (error instanceof ValkeyQueryException queryException) {
                observation.lowCardinalityKeyValue("valkey.error.code", queryException.errorCodeValue());
                observation.lowCardinalityKeyValue("valkey.error.category", queryException.errorCategory());
            } else if (error != null) {
                observation.lowCardinalityKeyValue("valkey.error.code", "unknown");
                observation.lowCardinalityKeyValue("valkey.error.category", "unknown");
            }
            if (error != null) {
                recordFailureMetric(commandName, indexName, routeType, error);
            }
            if (slow) {
                recordSlowQuery(commandName, indexName, querySupplier, routeType, elapsedMs);
            }
            if (traceQueryTextEnabled || error != null) {
                String statement = resolveStatement(querySupplier);
                if (!statement.isBlank()) {
                    observation.highCardinalityKeyValue("db.statement", statement);
                }
            }
            observation.stop();
        }
    }

    private void recordSlowQueryIfNeeded(
            String commandName,
            String indexName,
            Supplier<String> querySupplier,
            String routeType,
            long startNanos) {
        if (!slowLogEnabled) {
            return;
        }
        long elapsedMs = elapsedMillis(startNanos);
        if (elapsedMs < slowQueryThresholdMs) {
            return;
        }
        recordSlowQuery(commandName, indexName, querySupplier, routeType, elapsedMs);
    }

    private void recordSlowQuery(
            String commandName,
            String indexName,
            Supplier<String> querySupplier,
            String routeType,
            long elapsedMs) {
        String normalizedCommandName = normalize(commandName);
        String normalizedIndexName = normalize(indexName);
        String normalizedRouteType = normalize(routeType);
        commandMetricsRecorder.recordSlowQuery(normalizedCommandName, normalizedIndexName, normalizedRouteType);
        slowLogRecorder.record(new SlowLogEntry(
                normalizedCommandName,
                normalizedIndexName,
                normalizedRouteType,
                elapsedMs,
                resolveStatement(querySupplier)
        ));
    }

    private long elapsedMillis(long startNanos) {
        return Math.max(0L, (System.nanoTime() - startNanos) / 1_000_000L);
    }

    private String resolveStatement(Supplier<String> querySupplier) {
        if (querySupplier == null) {
            return "";
        }
        try {
            String statement = querySupplier.get();
            return statement == null ? "" : statement;
        } catch (RuntimeException exception) {
            return "";
        }
    }

    private String normalize(String value) {
        return value == null || value.isBlank() ? "unknown" : value;
    }

    private void recordFailureMetric(String commandName, String indexName, String routeType, Exception error) {
        if (!commandMetricsEnabled) {
            return;
        }
        String normalizedCommandName = normalize(commandName);
        String normalizedIndexName = normalize(indexName);
        String normalizedRouteType = normalize(routeType);
        if (error instanceof ValkeyQueryException queryException) {
            commandMetricsRecorder.recordFailure(
                    normalizedCommandName,
                    normalizedIndexName,
                    queryException.errorCodeValue(),
                    queryException.errorCategory(),
                    normalizedRouteType
            );
            return;
        }
        commandMetricsRecorder.recordFailure(
                normalizedCommandName,
                normalizedIndexName,
                "unknown",
                "unknown",
                normalizedRouteType
        );
    }
}
