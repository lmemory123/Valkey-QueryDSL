package com.momao.valkey.autoconfigure;

import com.momao.valkey.adapter.BaseValkeyRepository;
import com.momao.valkey.adapter.IndexDiff;
import com.momao.valkey.adapter.IndexDiffItem;
import com.momao.valkey.adapter.IndexDiffType;
import com.momao.valkey.core.metadata.IndexSchema;
import com.momao.valkey.core.metadata.SchemaField;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ValkeyIndexAutoCreatorTests {

    @Test
    void noneModeSkipsInspectionEntirely() {
        ValkeyQueryProperties properties = new ValkeyQueryProperties();
        properties.getIndexManagement().setMode(IndexManagementMode.NONE);
        ValkeyIndexAutoCreator autoCreator = new ValkeyIndexAutoCreator(
                new ValkeyIndexManager(null, new ValkeyQueryPackages(List.of()), properties, ValkeyIndexMetrics.noop()));

        StubRepository repository = new StubRepository(IndexDiff.of("idx:test", new IndexDiffItem(
                IndexDiffType.FIELD_MISSING,
                "price",
                "expected",
                null
        )));

        assertDoesNotThrow(() -> autoCreator.applyIndexManagement(repository));
        assertEquals(0, repository.inspectCalls());
        assertEquals(0, repository.createCalls());
        assertEquals(0, repository.dropCalls());
    }

    @Test
    void validateFailsWhenDiffExists() {
        ValkeyQueryProperties properties = new ValkeyQueryProperties();
        properties.getIndexManagement().setMode(IndexManagementMode.VALIDATE);
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        ValkeyIndexManager indexManager = new ValkeyIndexManager(null, new ValkeyQueryPackages(List.of()), properties, ValkeyIndexMetrics.of(meterRegistry));
        ValkeyIndexAutoCreator autoCreator = new ValkeyIndexAutoCreator(indexManager);

        StubRepository repository = new StubRepository(IndexDiff.of("idx:test", new IndexDiffItem(
                IndexDiffType.FIELD_MISSING,
                "price",
                "expected",
                null
        )));

        try {
            IllegalStateException exception = assertThrows(IllegalStateException.class, () -> autoCreator.applyIndexManagement(repository));
            assertTrue(exception.getMessage().contains("VALIDATE 模式已阻止启动"));
            assertEquals(1, repository.inspectCalls());
            assertEquals(1.0d, meterRegistry.get("valkey.query.index.diff").counter().count());
            assertEquals(1.0d, meterRegistry.get("valkey.query.index.validation.failed").counter().count());
            assertEquals(0, repository.createCalls());
        } finally {
            meterRegistry.close();
        }
    }

    @Test
    void validateFailsWhenIndexIsMissing() {
        ValkeyQueryProperties properties = new ValkeyQueryProperties();
        properties.getIndexManagement().setMode(IndexManagementMode.VALIDATE);
        ValkeyIndexAutoCreator autoCreator = new ValkeyIndexAutoCreator(
                new ValkeyIndexManager(null, new ValkeyQueryPackages(List.of()), properties, ValkeyIndexMetrics.noop()));

        StubRepository repository = new StubRepository(IndexDiff.of("idx:test", new IndexDiffItem(
                IndexDiffType.INDEX_MISSING,
                "idx:test",
                "present",
                "missing"
        )));

        IllegalStateException exception = assertThrows(IllegalStateException.class, () -> autoCreator.applyIndexManagement(repository));
        assertTrue(exception.getMessage().contains("VALIDATE 模式已阻止启动"));
        assertEquals(1, repository.inspectCalls());
        assertEquals(0, repository.createCalls());
        assertEquals(0, repository.dropCalls());
    }

    @Test
    void recreateDropsAndCreatesIndex() {
        ValkeyQueryProperties properties = new ValkeyQueryProperties();
        properties.getIndexManagement().setMode(IndexManagementMode.RECREATE);
        SimpleMeterRegistry meterRegistry = new SimpleMeterRegistry();
        ValkeyIndexManager indexManager = new ValkeyIndexManager(null, new ValkeyQueryPackages(List.of()), properties, ValkeyIndexMetrics.of(meterRegistry));
        ValkeyIndexAutoCreator autoCreator = new ValkeyIndexAutoCreator(indexManager);

        StubRepository repository = new StubRepository(IndexDiff.empty("idx:test"));

        try {
            assertDoesNotThrow(() -> autoCreator.applyIndexManagement(repository));
            assertEquals(0, repository.inspectCalls());
            assertEquals(1, repository.dropCalls());
            assertEquals(1, repository.createCalls());
            assertEquals(1.0d, meterRegistry.get("valkey.query.index.recreate").counter().count());
        } finally {
            meterRegistry.close();
        }
    }

    private static final class StubRepository extends BaseValkeyRepository<Object> {

        private final IndexDiff diff;
        private final AtomicInteger inspectCalls = new AtomicInteger();
        private final AtomicInteger createCalls = new AtomicInteger();
        private final AtomicInteger dropCalls = new AtomicInteger();

        private StubRepository(IndexDiff diff) {
            super(new IndexSchema(
                    "idx:test",
                    com.momao.valkey.annotation.StorageType.JSON,
                    List.of("test:"),
                    List.of(
                            SchemaField.tag("id", ",", false),
                            SchemaField.numeric("price", true)
                    )
            ), Object.class);
            this.diff = diff;
        }

        @Override
        public IndexDiff inspectIndexDiff() {
            inspectCalls.incrementAndGet();
            return diff;
        }

        @Override
        public String createIndex() {
            createCalls.incrementAndGet();
            return "OK";
        }

        @Override
        public String dropIndex(boolean deleteDocuments) {
            dropCalls.incrementAndGet();
            return "OK";
        }

        private int inspectCalls() {
            return inspectCalls.get();
        }

        private int createCalls() {
            return createCalls.get();
        }

        private int dropCalls() {
            return dropCalls.get();
        }
    }
}
