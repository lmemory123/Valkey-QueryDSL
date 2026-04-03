package com.momao.valkey.adapter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public final class IndexDiff {

    private final String indexName;
    private final List<IndexDiffItem> items;

    private IndexDiff(String indexName, List<IndexDiffItem> items) {
        this.indexName = indexName;
        this.items = List.copyOf(items);
    }

    public static IndexDiff empty(String indexName) {
        return new IndexDiff(indexName, List.of());
    }

    public static IndexDiff of(String indexName, Collection<IndexDiffItem> items) {
        return new IndexDiff(indexName, new ArrayList<>(items));
    }

    public static IndexDiff of(String indexName, IndexDiffItem item) {
        return new IndexDiff(indexName, List.of(item));
    }

    public String indexName() {
        return indexName;
    }

    public List<IndexDiffItem> items() {
        return items;
    }

    public boolean isEmpty() {
        return items.isEmpty();
    }

    public boolean onlyMissingIndex() {
        return !items.isEmpty() && items.stream().allMatch(item -> item.type() == IndexDiffType.INDEX_MISSING);
    }

    public boolean requiresRecreate() {
        return !isEmpty() && !onlyMissingIndex();
    }

    public boolean contains(IndexDiffType type) {
        return items.stream().anyMatch(item -> item.type() == type);
    }

    public IndexMigrationPlan plan() {
        if (items.isEmpty()) {
            return new IndexMigrationPlan(indexName, IndexPlanAction.NONE, List.of());
        }
        if (onlyMissingIndex()) {
            return new IndexMigrationPlan(indexName, IndexPlanAction.CREATE, items);
        }
        return new IndexMigrationPlan(indexName, IndexPlanAction.RECREATE, items);
    }

    public String summary() {
        return plan().summary();
    }
}
