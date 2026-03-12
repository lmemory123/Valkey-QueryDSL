package com.momao.valkey.core;

import java.util.List;

public record SearchResult<T>(long total, List<T> records) {

    public SearchResult(long total, List<T> records) {
        this.total = total;
        this.records = records == null ? List.of() : List.copyOf(records);
    }
}
