package com.momao.valkey.core;

import java.util.Arrays;
import java.util.List;

public record BulkUpdateItem(
        String id,
        List<UpdateOperation> operations,
        SearchPredicate predicate
) {

    public BulkUpdateItem {
        if (id == null || id.isBlank()) {
            throw new IllegalArgumentException("Bulk update id cannot be blank");
        }
        if (operations == null || operations.isEmpty()) {
            throw new IllegalArgumentException("Bulk update operations cannot be empty");
        }
        operations = List.copyOf(operations);
    }

    public static BulkUpdateItem of(String id, UpdateOperation... operations) {
        if (operations == null || operations.length == 0) {
            throw new IllegalArgumentException("Bulk update operations cannot be empty");
        }
        return new BulkUpdateItem(id, Arrays.asList(operations), null);
    }

    public static BulkUpdateItem of(String id, List<UpdateOperation> operations) {
        return new BulkUpdateItem(id, operations, null);
    }

    public BulkUpdateItem when(SearchCondition condition) {
        return new BulkUpdateItem(id, operations, extractPredicate(condition));
    }

    private static SearchPredicate extractPredicate(SearchCondition condition) {
        if (condition == null) {
            return null;
        }
        SearchPredicate extracted = condition.predicate();
        if (extracted == null) {
            throw new IllegalArgumentException("Bulk update only supports structured equality predicates");
        }
        return extracted;
    }
}
