package com.momao.valkey.core;

public record FacetBucket(
        String value,
        long count
) {
}
