package com.momao.valkey.core;

import java.util.List;

public record Page<T>(long total, List<T> records) {

    public Page(long total, List<T> records) {
        this.total = total;
        this.records = records == null ? List.of() : List.copyOf(records);
    }
}
