package com.momao.valkey.autoconfigure;

import com.momao.valkey.adapter.observability.SlowLogEntry;
import com.momao.valkey.adapter.observability.SlowLogRecorder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

final class LoggingSlowLogRecorder implements SlowLogRecorder {

    private static final Logger log = LoggerFactory.getLogger(LoggingSlowLogRecorder.class);

    @Override
    public void record(SlowLogEntry entry) {
        if (entry.statement() == null || entry.statement().isBlank()) {
            log.warn("[valkey-query][slow] {} ms | command={} | index={} | route={}",
                    entry.elapsedMs(),
                    entry.commandName(),
                    entry.indexName(),
                    entry.routeType());
            return;
        }
        log.warn("[valkey-query][slow] {} ms | command={} | index={} | route={} | statement={}",
                entry.elapsedMs(),
                entry.commandName(),
                entry.indexName(),
                entry.routeType(),
                entry.statement());
    }
}
