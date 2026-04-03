package com.momao.valkey.adapter;

import com.momao.valkey.adapter.observability.ValkeyObservationInvoker;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ValkeyScriptExecutorTests {

    @Test
    void fallsBackToEvalWhenServerDoesNotKnowSha() throws Exception {
        RecordingRouting routing = new RecordingRouting(true);
        ValkeyScriptExecutor executor = new ValkeyScriptExecutor();

        Object result = executor.executeWrite(
                routing,
                ValkeyObservationInvoker.noop(),
                "idx:test",
                () -> "EVALSHA <script>",
                "return 1",
                1,
                List.of("test:key", "arg-1")
        );

        assertEquals(1L, result);
        assertEquals(List.of(
                List.of("EVALSHA", "e0e1f9fabfc9d4800c877a703b823ac0578ff8db", "1", "test:key", "arg-1"),
                List.of("EVAL", "return 1", "1", "test:key", "arg-1")
        ), routing.commands());
    }

    @Test
    void retriesWithEvalShaFirstEvenAfterNoscriptFallbackWasUsed() throws Exception {
        RecordingRouting routing = new RecordingRouting(true);
        ValkeyScriptExecutor executor = new ValkeyScriptExecutor();

        executor.executeWrite(
                routing,
                ValkeyObservationInvoker.noop(),
                "idx:test",
                () -> "EVALSHA <script>",
                "return 1",
                1,
                List.of("test:key")
        );
        routing.clear();

        Object result = executor.executeWrite(
                routing,
                ValkeyObservationInvoker.noop(),
                "idx:test",
                () -> "EVALSHA <script>",
                "return 1",
                1,
                List.of("test:key")
        );

        assertEquals(1L, result);
        assertEquals(List.of(
                List.of("EVALSHA", "e0e1f9fabfc9d4800c877a703b823ac0578ff8db", "1", "test:key")
        ), routing.commands());
    }

    @Test
    void evictsLeastRecentlyUsedScriptWhenCacheReachesCapacity() throws Exception {
        RecordingRouting routing = new RecordingRouting(false);
        ValkeyScriptExecutor executor = new ValkeyScriptExecutor(2);

        executor.executeWrite(routing, ValkeyObservationInvoker.noop(), "idx:test", () -> "script-1", "return 1", 1, List.of("key-1"));
        executor.executeWrite(routing, ValkeyObservationInvoker.noop(), "idx:test", () -> "script-2", "return 2", 1, List.of("key-2"));
        executor.executeWrite(routing, ValkeyObservationInvoker.noop(), "idx:test", () -> "script-1", "return 1", 1, List.of("key-1"));
        executor.executeWrite(routing, ValkeyObservationInvoker.noop(), "idx:test", () -> "script-3", "return 3", 1, List.of("key-3"));

        assertEquals(2, executor.cacheSize());
        assertEquals(true, executor.isCached("return 1"));
        assertEquals(false, executor.isCached("return 2"));
        assertEquals(true, executor.isCached("return 3"));
    }

    private static final class RecordingRouting implements ValkeyClientRouting {

        private final boolean failFirstEvalSha;
        private final List<List<String>> commands = new ArrayList<>();
        private boolean firstEvalShaSeen = false;

        private RecordingRouting(boolean failFirstEvalSha) {
            this.failFirstEvalSha = failFirstEvalSha;
        }

        @Override
        public Object executeWrite(String[] command) {
            commands.add(List.copyOf(java.util.Arrays.asList(command.clone())));
            if ("EVALSHA".equals(command[0]) && failFirstEvalSha && !firstEvalShaSeen) {
                firstEvalShaSeen = true;
                throw new RuntimeException("NOSCRIPT No matching script. Please use EVAL.");
            }
            return 1L;
        }

        @Override
        public Object executeRead(String[] command) {
            return null;
        }

        private List<List<String>> commands() {
            return commands;
        }

        private void clear() {
            commands.clear();
        }
    }
}
