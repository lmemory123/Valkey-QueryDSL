package com.momao.valkey.core;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public record AggregateResult(
        long total,
        List<AggregateRow> rows
) {

    public AggregateResult {
        rows = rows == null ? List.of() : List.copyOf(rows);
    }

    public FacetResult toFacet(String fieldName) {
        return toFacet(fieldName, "count");
    }

    public FacetResult toFacet(String fieldName, String countAlias) {
        List<FacetBucket> buckets = new ArrayList<>(rows.size());
        for (AggregateRow row : rows) {
            String value = row.getString(fieldName);
            Long count = row.getLong(countAlias);
            buckets.add(new FacetBucket(value, count == null ? 0L : count));
        }
        return new FacetResult(fieldName, total, buckets);
    }

    public <R> List<R> mapRows(Function<AggregateRow, R> mapper) {
        if (mapper == null) {
            throw new IllegalArgumentException("Aggregate row mapper cannot be null");
        }
        List<R> mapped = new ArrayList<>(rows.size());
        for (AggregateRow row : rows) {
            mapped.add(mapper.apply(row));
        }
        return mapped;
    }

    public <R> List<R> mapRows(Class<R> type) {
        return mapRows(row -> row.toObject(type));
    }
}
