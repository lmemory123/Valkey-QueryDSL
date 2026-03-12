package com.momao.valkey.core;

import com.momao.valkey.core.metadata.SchemaField;

import java.util.List;

public interface ValkeyRepository<T> {

    String checkAndCreateIndex();

    void save(String id, T entity);

    SearchResult<T> search(SearchCondition condition);

    List<T> list(SearchCondition condition);

    Page<T> page(SearchCondition condition, int offset, int limit);

    T one(SearchCondition condition);

    long count(SearchCondition condition);

    String getIndexName();

    String getPrefix();

    List<String> getPrefixes();

    List<SchemaField> getFields();

    default ValkeyQueryChain<T> queryChain() {
        return new ValkeyQueryChain<>(this);
    }
}