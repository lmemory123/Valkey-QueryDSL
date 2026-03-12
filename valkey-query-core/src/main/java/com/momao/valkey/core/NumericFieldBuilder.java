package com.momao.valkey.core;

public final class NumericFieldBuilder {

    private final String fieldName;

    public NumericFieldBuilder(String fieldName) {
        this.fieldName = fieldName;
    }

    public SearchCondition eq(Number value) {
        return new SearchCondition("@" + fieldName + ":[" + value + " " + value + "]");
    }

    public SearchCondition gt(Number value) {
        return new SearchCondition("@" + fieldName + ":[(" + value + " +inf]");
    }

    public SearchCondition gte(Number value) {
        return new SearchCondition("@" + fieldName + ":[" + value + " +inf]");
    }

    public SearchCondition lt(Number value) {
        return new SearchCondition("@" + fieldName + ":[-inf (" + value + ")]");
    }

    public SearchCondition lte(Number value) {
        return new SearchCondition("@" + fieldName + ":[-inf " + value + "]");
    }

    public SearchCondition between(Number from, Number to) {
        return new SearchCondition("@" + fieldName + ":[" + from + " " + to + "]");
    }

    public SearchCondition in(Number... values) {
        if (values == null || values.length == 0) {
            throw new IllegalArgumentException("Values cannot be empty");
        }
        StringBuilder sb = new StringBuilder("@" + fieldName + ":(");
        for (int i = 0; i < values.length; i++) {
            if (i > 0) {
                sb.append(" | ");
            }
            sb.append("[").append(values[i]).append(" ").append(values[i]).append("]");
        }
        sb.append(")");
        return new SearchCondition(sb.toString());
    }
}
