package com.momao.valkey.core;

import com.momao.valkey.core.metadata.SchemaField;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertSame;

class ValkeyRepositoryDefaultMethodsTests {

    @Test
    void saveBatchDelegatesToSaveAll() {
        RecordingRepository repository = new RecordingRepository();
        List<BulkSaveItem<String>> items = List.of(new BulkSaveItem<>("1", "value"));
        BulkWriteOptions options = BulkWriteOptions.unordered();

        repository.saveAll(items, options);

        assertSame(items, repository.lastItems);
        assertSame(options, repository.lastOptions);
    }

    private static final class RecordingRepository implements ValkeyRepository<String> {

        private List<BulkSaveItem<String>> lastItems;
        private BulkWriteOptions lastOptions;

        @Override
        public String checkAndCreateIndex() {
            return "OK";
        }

        @Override
        public void save(String id, String entity) {
        }

        @Override
        public BulkWriteResult saveAll(List<BulkSaveItem<String>> items, BulkWriteOptions options) {
            this.lastItems = items;
            this.lastOptions = options;
            return new BulkWriteResult(items.size(), items.size(), 0, false, List.of());
        }

        @Override
        public long updateById(Object id, List<UpdateOperation> operations, SearchPredicate predicate) {
            return 0;
        }

        @Override
        public SearchResult<String> search(SearchCondition condition) {
            throw new UnsupportedOperationException();
        }

        @Override
        public List<String> list(SearchCondition condition) {
            throw new UnsupportedOperationException();
        }

        @Override
        public Page<String> page(SearchCondition condition, int offset, int limit) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String one(SearchCondition condition) {
            throw new UnsupportedOperationException();
        }

        @Override
        public long count(SearchCondition condition) {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getIndexName() {
            return "idx:test";
        }

        @Override
        public String getPrefix() {
            return "test:";
        }

        @Override
        public List<String> getPrefixes() {
            return List.of("test:");
        }

        @Override
        public List<SchemaField> getFields() {
            return List.of();
        }
    }
}
