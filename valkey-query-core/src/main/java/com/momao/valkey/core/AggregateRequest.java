package com.momao.valkey.core;

import java.util.List;

public record AggregateRequest(
        List<String> groupByFields,
        List<AggregateReducer> reducers,
        List<AggregateApply> applies,
        List<String> filters,
        String sortField,
        boolean sortAscending,
        int offset,
        int limit
) {

    private static final int DEFAULT_LIMIT = 1_000;

    public AggregateRequest {
        groupByFields = groupByFields == null ? List.of() : List.copyOf(groupByFields);
        reducers = reducers == null ? List.of() : List.copyOf(reducers);
        applies = applies == null ? List.of() : List.copyOf(applies);
        filters = filters == null ? List.of() : List.copyOf(filters);
        offset = Math.max(0, offset);
        limit = limit <= 0 ? DEFAULT_LIMIT : limit;
    }

    public AggregateRequest(
            List<String> groupByFields,
            List<AggregateReducer> reducers,
            String sortField,
            boolean sortAscending,
            int offset,
            int limit
    ) {
        this(groupByFields, reducers, List.of(), List.of(), sortField, sortAscending, offset, limit);
    }

    public boolean hasGroupBy() {
        return !groupByFields.isEmpty();
    }

    public boolean hasReducers() {
        return !reducers.isEmpty();
    }

    public boolean hasApplies() {
        return !applies.isEmpty();
    }

    public boolean hasFilters() {
        return !filters.isEmpty();
    }

    public boolean hasSort() {
        return sortField != null && !sortField.isBlank();
    }
}
