package com.momao.valkey.adapter;

import java.util.List;

public record IndexMigrationPlan(
        String indexName,
        IndexPlanAction action,
        List<IndexDiffItem> items
) {

    public IndexMigrationPlan {
        items = List.copyOf(items);
    }

    public boolean isEmpty() {
        return action == IndexPlanAction.NONE || items.isEmpty();
    }

    public boolean requiresCreate() {
        return action == IndexPlanAction.CREATE;
    }

    public boolean requiresRecreate() {
        return action == IndexPlanAction.RECREATE;
    }

    public String summary() {
        if (isEmpty()) {
            return "index=" + indexName + ",action=NONE,status=match";
        }
        return "index=" + indexName
                + ",action=" + action
                + ",diffs=" + items.stream()
                .map(IndexDiffItem::summary)
                .reduce((left, right) -> left + " | " + right)
                .orElse("none");
    }
}
