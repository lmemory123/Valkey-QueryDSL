package com.momao.valkey.core;

import java.util.List;

public record BulkWriteResult(
        int submitted,
        int succeeded,
        int failed,
        boolean partialSuccess,
        List<BulkWriteItemResult> items
) {

    public BulkWriteResult {
        items = items == null ? List.of() : List.copyOf(items);
    }
}
