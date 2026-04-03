package com.momao.valkey.core;

import com.momao.valkey.core.metadata.SchemaField;

import java.util.List;

public interface ValkeyRepository<T> {

    String checkAndCreateIndex();

    void save(String id, T entity);

    default BulkWriteResult saveAll(List<BulkSaveItem<T>> items) {
        return saveAll(items, BulkWriteOptions.defaults());
    }

    default BulkWriteResult saveAll(List<BulkSaveItem<T>> items, BulkWriteOptions options) {
        throw new UnsupportedOperationException("Bulk save is not implemented");
    }

    default BulkWriteResult deleteAll(List<String> ids) {
        return deleteAll(ids, BulkWriteOptions.defaults());
    }

    default BulkWriteResult deleteAll(List<String> ids, BulkWriteOptions options) {
        throw new UnsupportedOperationException("Bulk delete is not implemented");
    }

    default BulkWriteResult updateAll(List<BulkUpdateItem> items) {
        return updateAll(items, BulkWriteOptions.defaults());
    }

    default BulkWriteResult updateAll(List<BulkUpdateItem> items, BulkWriteOptions options) {
        throw new UnsupportedOperationException("Bulk update is not implemented");
    }

    default long updateById(Object id, List<UpdateOperation> operations) {
        return updateById(id, operations, null);
    }

    long updateById(Object id, List<UpdateOperation> operations, SearchPredicate predicate);

    SearchResult<T> search(SearchCondition condition);

    List<T> list(SearchCondition condition);

    Page<T> page(SearchCondition condition, int offset, int limit);

    T one(SearchCondition condition);

    long count(SearchCondition condition);

    default AggregateResult aggregate(SearchCondition condition, AggregateRequest request) {
        throw new UnsupportedOperationException("Aggregate query is not implemented");
    }

    String getIndexName();

    String getPrefix();

    List<String> getPrefixes();

    List<SchemaField> getFields();

    default ValkeyQueryChain<T> queryChain() {
        return new ValkeyQueryChain<>(this);
    }

    default ValkeyUpdateChain<T> updateChain() {
        return new ValkeyUpdateChain<>(this);
    }

    default ValkeyAggregateChain<T> aggregateChain() {
        return new ValkeyAggregateChain<>(this);
    }

    default ValkeyFacetChain<T> facetChain() {
        return new ValkeyFacetChain<>(this);
    }
}
