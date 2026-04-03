package com.momao.valkey.core;

import java.math.BigDecimal;

public record NumericUpdateOperation(String fieldName, Number delta, UpdateOperationKind kind) implements UpdateOperation {

    public NumericUpdateOperation {
        if (fieldName == null || fieldName.isBlank()) {
            throw new IllegalArgumentException("Field name cannot be blank");
        }
        if (delta == null) {
            throw new IllegalArgumentException("Numeric delta cannot be null");
        }
        if (kind != UpdateOperationKind.INCREMENT && kind != UpdateOperationKind.DECREMENT) {
            throw new IllegalArgumentException("Numeric update kind must be increment or decrement");
        }
        if (BigDecimal.ZERO.compareTo(toBigDecimal(delta)) > 0) {
            throw new IllegalArgumentException("Numeric delta cannot be negative");
        }
    }

    public Number signedDelta() {
        if (kind == UpdateOperationKind.INCREMENT) {
            return delta;
        }
        BigDecimal value = toBigDecimal(delta).negate();
        return scaleAwareNumber(delta, value);
    }

    private static BigDecimal toBigDecimal(Number value) {
        return new BigDecimal(String.valueOf(value));
    }

    private static Number scaleAwareNumber(Number original, BigDecimal value) {
        if (original instanceof Byte || original instanceof Short || original instanceof Integer) {
            return value.intValue();
        }
        if (original instanceof Long) {
            return value.longValue();
        }
        if (original instanceof Float) {
            return value.floatValue();
        }
        if (original instanceof Double) {
            return value.doubleValue();
        }
        return value.stripTrailingZeros().scale() <= 0 ? value.longValue() : value.doubleValue();
    }
}
