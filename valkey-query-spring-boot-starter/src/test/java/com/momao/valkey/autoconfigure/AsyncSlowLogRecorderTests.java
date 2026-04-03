package com.momao.valkey.autoconfigure;

import com.momao.valkey.adapter.observability.SlowLogEntry;
import com.momao.valkey.adapter.observability.SlowLogRecorder;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class AsyncSlowLogRecorderTests {

    @Test
    void recordsSlowLogOnDedicatedBackgroundThread() throws Exception {
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<String> threadName = new AtomicReference<>();
        SlowLogRecorder delegate = entry -> {
            threadName.set(Thread.currentThread().getName());
            latch.countDown();
        };

        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        try (AsyncSlowLogRecorder recorder = new AsyncSlowLogRecorder(
                delegate,
                "valkey-query-test-slowlog",
                io.micrometer.core.instrument.Counter.builder("valkey.query.slowlog.dropped").register(meterRegistry))) {
            recorder.record(new SlowLogEntry("FT.SEARCH", "idx:test", "read", 220L, "@title:test"));
            assertTrue(latch.await(2L, TimeUnit.SECONDS));
        } finally {
            meterRegistry.close();
        }

        assertEquals("valkey-query-test-slowlog", threadName.get());
    }

    @Test
    void incrementsDroppedCounterWhenQueueIsFull() throws Exception {
        CountDownLatch runningLatch = new CountDownLatch(1);
        CountDownLatch releaseLatch = new CountDownLatch(1);
        SlowLogRecorder blockingDelegate = entry -> {
            runningLatch.countDown();
            try {
                assertTrue(releaseLatch.await(2L, TimeUnit.SECONDS));
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(exception);
            }
        };

        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        try (AsyncSlowLogRecorder recorder = new AsyncSlowLogRecorder(
                blockingDelegate,
                "valkey-query-test-slowlog",
                1,
                io.micrometer.core.instrument.Counter.builder("valkey.query.slowlog.dropped").register(meterRegistry))) {
            recorder.record(new SlowLogEntry("FT.SEARCH", "idx:test", "read", 220L, "1"));
            assertTrue(runningLatch.await(2L, TimeUnit.SECONDS));
            recorder.record(new SlowLogEntry("FT.SEARCH", "idx:test", "read", 221L, "2"));
            recorder.record(new SlowLogEntry("FT.SEARCH", "idx:test", "read", 222L, "3"));
            releaseLatch.countDown();
            Thread.sleep(100L);
            assertEquals(1.0d, meterRegistry.get("valkey.query.slowlog.dropped").counter().count());
        } finally {
            meterRegistry.close();
        }
    }
}
