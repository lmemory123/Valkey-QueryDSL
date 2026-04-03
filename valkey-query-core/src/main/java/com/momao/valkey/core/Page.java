package com.momao.valkey.core;

import java.util.List;
import java.util.Objects;
import java.util.function.Function;

public record Page<T>(long total, List<T> records) {

    public Page(long total, List<T> records) {
        this.total = total;
        this.records = records == null ? List.of() : List.copyOf(records);
    }

    public <R> Page<R> map(Function<T, R> mapper) {
        Objects.requireNonNull(mapper, "mapper");
        List<R> mapped = new java.util.ArrayList<>(records.size());
        for (T record : records) {
            mapped.add(mapper.apply(record));
        }
        return new Page<>(total, mapped);
    }
}
