package com.momao.valkey.core;

public interface AggregateExpression {

    String render();

    default AggregateExpression plus(AggregateExpression other) {
        return AggregateExpressions.binary(this, "+", other);
    }

    default AggregateExpression plus(Number other) {
        return plus(AggregateExpressions.number(other));
    }

    default AggregateExpression minus(AggregateExpression other) {
        return AggregateExpressions.binary(this, "-", other);
    }

    default AggregateExpression minus(Number other) {
        return minus(AggregateExpressions.number(other));
    }

    default AggregateExpression multiply(AggregateExpression other) {
        return AggregateExpressions.binary(this, "*", other);
    }

    default AggregateExpression multiply(Number other) {
        return multiply(AggregateExpressions.number(other));
    }

    default AggregateExpression divide(AggregateExpression other) {
        return AggregateExpressions.binary(this, "/", other);
    }

    default AggregateExpression divide(Number other) {
        return divide(AggregateExpressions.number(other));
    }

    default AggregatePredicate eq(AggregateExpression other) {
        return AggregateExpressions.compare(this, "==", other);
    }

    default AggregatePredicate eq(Number other) {
        return eq(AggregateExpressions.number(other));
    }

    default AggregatePredicate ne(AggregateExpression other) {
        return AggregateExpressions.compare(this, "!=", other);
    }

    default AggregatePredicate ne(Number other) {
        return ne(AggregateExpressions.number(other));
    }

    default AggregatePredicate gt(AggregateExpression other) {
        return AggregateExpressions.compare(this, ">", other);
    }

    default AggregatePredicate gt(Number other) {
        return gt(AggregateExpressions.number(other));
    }

    default AggregatePredicate gte(AggregateExpression other) {
        return AggregateExpressions.compare(this, ">=", other);
    }

    default AggregatePredicate gte(Number other) {
        return gte(AggregateExpressions.number(other));
    }

    default AggregatePredicate lt(AggregateExpression other) {
        return AggregateExpressions.compare(this, "<", other);
    }

    default AggregatePredicate lt(Number other) {
        return lt(AggregateExpressions.number(other));
    }

    default AggregatePredicate lte(AggregateExpression other) {
        return AggregateExpressions.compare(this, "<=", other);
    }

    default AggregatePredicate lte(Number other) {
        return lte(AggregateExpressions.number(other));
    }
}
