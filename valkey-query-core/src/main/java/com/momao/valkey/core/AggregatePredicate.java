package com.momao.valkey.core;

public interface AggregatePredicate {

    String render();

    default AggregatePredicate and(AggregatePredicate other) {
        return AggregateExpressions.logical(this, "&&", other);
    }

    default AggregatePredicate or(AggregatePredicate other) {
        return AggregateExpressions.logical(this, "||", other);
    }
}
