package com.momao.valkey.adapter.observability;

public record SlowLogEntry(
        String commandName,
        String indexName,
        String routeType,
        long elapsedMs,
        String statement
) {
}
