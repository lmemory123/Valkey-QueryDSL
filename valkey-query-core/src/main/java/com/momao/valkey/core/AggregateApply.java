package com.momao.valkey.core;

public record AggregateApply(
        String expression,
        String alias
) {

    public AggregateApply {
        if (expression == null || expression.isBlank()) {
            throw new IllegalArgumentException("Aggregate apply expression cannot be blank");
        }
        if (alias == null || alias.isBlank()) {
            throw new IllegalArgumentException("Aggregate apply alias cannot be blank");
        }
    }
}
