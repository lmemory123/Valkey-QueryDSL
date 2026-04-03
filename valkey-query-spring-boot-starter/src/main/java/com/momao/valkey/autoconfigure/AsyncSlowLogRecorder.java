package com.momao.valkey.autoconfigure;

import com.momao.valkey.adapter.observability.SlowLogEntry;
import com.momao.valkey.adapter.observability.SlowLogRecorder;
import io.micrometer.core.instrument.Counter;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

final class AsyncSlowLogRecorder implements SlowLogRecorder {

    private static final int DEFAULT_QUEUE_CAPACITY = 1024;

    private final SlowLogRecorder delegate;
    private final ThreadPoolExecutor executor;

    AsyncSlowLogRecorder(SlowLogRecorder delegate, String threadName, Counter droppedCounter) {
        this(delegate, threadName, DEFAULT_QUEUE_CAPACITY, droppedCounter);
    }

    AsyncSlowLogRecorder(SlowLogRecorder delegate, String threadName, int queueCapacity, Counter droppedCounter) {
        this.delegate = delegate == null ? SlowLogRecorder.NOOP : delegate;
        this.executor = new ThreadPoolExecutor(
                1,
                1,
                0L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(Math.max(1, queueCapacity)),
                daemonThreadFactory(threadName),
                rejectedExecutionHandler(droppedCounter)
        );
    }

    @Override
    public void record(SlowLogEntry entry) {
        executor.execute(() -> delegate.record(entry));
    }

    @Override
    public void close() {
        executor.shutdown();
        delegate.close();
    }

    private ThreadFactory daemonThreadFactory(String threadName) {
        String resolvedName = (threadName == null || threadName.isBlank()) ? "valkey-query-slowlog" : threadName;
        return runnable -> {
            Thread thread = new Thread(runnable, resolvedName);
            thread.setDaemon(true);
            return thread;
        };
    }

    private RejectedExecutionHandler rejectedExecutionHandler(Counter droppedCounter) {
        return (runnable, executor) -> {
            if (droppedCounter != null) {
                droppedCounter.increment();
            }
        };
    }
}
