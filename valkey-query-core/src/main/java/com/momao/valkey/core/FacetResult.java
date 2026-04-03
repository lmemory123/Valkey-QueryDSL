package com.momao.valkey.core;

import java.util.List;

public record FacetResult(
        String fieldName,
        long total,
        List<FacetBucket> buckets
) {

    public FacetResult {
        buckets = buckets == null ? List.of() : List.copyOf(buckets);
    }
}
