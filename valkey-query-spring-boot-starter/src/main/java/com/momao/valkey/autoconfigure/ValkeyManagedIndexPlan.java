package com.momao.valkey.autoconfigure;

import com.momao.valkey.adapter.IndexMigrationPlan;
import com.momao.valkey.core.metadata.IndexSchema;

public record ValkeyManagedIndexPlan(
        IndexSchema schema,
        IndexMigrationPlan plan
) {

    public String summary() {
        return "index=" + schema.indexName()
                + ",alias=" + schema.aliasName()
                + ",plan=" + plan.summary();
    }
}
