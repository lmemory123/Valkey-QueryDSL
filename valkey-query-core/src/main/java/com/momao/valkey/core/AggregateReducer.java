package com.momao.valkey.core;

public record AggregateReducer(
        AggregateReducerKind kind,
        String fieldName,
        String alias
) {

    public AggregateReducer {
        if (kind == null) {
            throw new IllegalArgumentException("Aggregate reducer kind cannot be null");
        }
        if (alias == null || alias.isBlank()) {
            throw new IllegalArgumentException("Aggregate reducer alias cannot be blank");
        }
        if (kind != AggregateReducerKind.COUNT && (fieldName == null || fieldName.isBlank())) {
            throw new IllegalArgumentException("Aggregate reducer field cannot be blank");
        }
    }

    public static AggregateReducer count(String alias) {
        return new AggregateReducer(AggregateReducerKind.COUNT, null, alias);
    }

    public static AggregateReducer countDistinct(String fieldName, String alias) {
        return new AggregateReducer(AggregateReducerKind.COUNT_DISTINCT, fieldName, alias);
    }

    public static AggregateReducer sum(String fieldName, String alias) {
        return new AggregateReducer(AggregateReducerKind.SUM, fieldName, alias);
    }

    public static AggregateReducer avg(String fieldName, String alias) {
        return new AggregateReducer(AggregateReducerKind.AVG, fieldName, alias);
    }

    public static AggregateReducer min(String fieldName, String alias) {
        return new AggregateReducer(AggregateReducerKind.MIN, fieldName, alias);
    }

    public static AggregateReducer max(String fieldName, String alias) {
        return new AggregateReducer(AggregateReducerKind.MAX, fieldName, alias);
    }
}
