package com.momao.valkey.core;

public final class AggregateExpressions {

    private AggregateExpressions() {
    }

    public static AggregateExpression field(String name) {
        if (name == null || name.isBlank()) {
            throw new IllegalArgumentException("Aggregate field name cannot be blank");
        }
        return new FieldExpression(name);
    }

    public static AggregateExpression field(ValkeyFieldReference field) {
        if (field == null) {
            throw new IllegalArgumentException("Aggregate field reference cannot be null");
        }
        return field(field.fieldName());
    }

    public static AggregateExpression number(Number value) {
        if (value == null) {
            throw new IllegalArgumentException("Aggregate numeric literal cannot be null");
        }
        return new LiteralExpression(String.valueOf(value));
    }

    static AggregateExpression binary(AggregateExpression left, String operator, AggregateExpression right) {
        if (left == null || right == null) {
            throw new IllegalArgumentException("Aggregate expression operands cannot be null");
        }
        return new BinaryExpression(left, operator, right);
    }

    static AggregatePredicate compare(AggregateExpression left, String operator, AggregateExpression right) {
        if (left == null || right == null) {
            throw new IllegalArgumentException("Aggregate predicate operands cannot be null");
        }
        return new ComparisonPredicate(left, operator, right);
    }

    static AggregatePredicate logical(AggregatePredicate left, String operator, AggregatePredicate right) {
        if (left == null || right == null) {
            throw new IllegalArgumentException("Aggregate predicates cannot be null");
        }
        return new LogicalPredicate(left, operator, right);
    }

    private record FieldExpression(String name) implements AggregateExpression {
        @Override
        public String render() {
            return "@" + name;
        }
    }

    private record LiteralExpression(String value) implements AggregateExpression {
        @Override
        public String render() {
            return value;
        }
    }

    private record BinaryExpression(AggregateExpression left, String operator, AggregateExpression right) implements AggregateExpression {
        @Override
        public String render() {
            return left.render() + " " + operator + " " + right.render();
        }
    }

    private record ComparisonPredicate(AggregateExpression left, String operator, AggregateExpression right) implements AggregatePredicate {
        @Override
        public String render() {
            return left.render() + " " + operator + " " + right.render();
        }
    }

    private record LogicalPredicate(AggregatePredicate left, String operator, AggregatePredicate right) implements AggregatePredicate {
        @Override
        public String render() {
            return "(" + left.render() + ") " + operator + " (" + right.render() + ")";
        }
    }
}
