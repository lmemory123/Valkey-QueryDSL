package com.momao.valkey.adapter.observability;

public interface SlowLogRecorder extends AutoCloseable {

    SlowLogRecorder NOOP = entry -> {
    };

    void record(SlowLogEntry entry);

    @Override
    default void close() {
    }
}
