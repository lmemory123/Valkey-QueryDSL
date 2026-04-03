package com.momao.valkey.adapter;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class IndexMigrationPlanTests {

    @Test
    void missingIndexProducesCreatePlan() {
        IndexDiff diff = IndexDiff.of("idx:test", new IndexDiffItem(
                IndexDiffType.INDEX_MISSING,
                "idx:test",
                "present",
                "missing"
        ));

        IndexMigrationPlan plan = diff.plan();

        assertEquals(IndexPlanAction.CREATE, plan.action());
        assertTrue(plan.requiresCreate());
        assertTrue(plan.summary().contains("action=CREATE"));
    }

    @Test
    void schemaMismatchProducesRecreatePlan() {
        IndexDiff diff = IndexDiff.of("idx:test", new IndexDiffItem(
                IndexDiffType.FIELD_DEFINITION_MISMATCH,
                "price",
                "expected",
                "actual"
        ));

        IndexMigrationPlan plan = diff.plan();

        assertEquals(IndexPlanAction.RECREATE, plan.action());
        assertTrue(plan.requiresRecreate());
        assertTrue(plan.summary().contains("action=RECREATE"));
    }
}
