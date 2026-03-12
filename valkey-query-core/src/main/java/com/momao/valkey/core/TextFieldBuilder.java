package com.momao.valkey.core;

public final class TextFieldBuilder {

    private final String fieldName;

    public TextFieldBuilder(String fieldName) {
        this.fieldName = fieldName;
    }

    public SearchCondition eq(String value) {
        return new SearchCondition("@" + fieldName + ":" + ValkeySyntaxUtils.escape(value));
    }

    public SearchCondition matches(String pattern) {
        return new SearchCondition("@" + fieldName + ":" + pattern);
    }

    public SearchCondition in(String... values) {
        if (values == null || values.length == 0) {
            throw new IllegalArgumentException("Values cannot be empty");
        }
        StringBuilder sb = new StringBuilder("@" + fieldName + ":(");
        for (int i = 0; i < values.length; i++) {
            if (i > 0) {
                sb.append(" | ");
            }
            sb.append(ValkeySyntaxUtils.escape(values[i]));
        }
        sb.append(")");
        return new SearchCondition(sb.toString());
    }

    public SearchCondition startsWith(String prefix) {
        return new SearchCondition("@" + fieldName + ":" + ValkeySyntaxUtils.escape(prefix) + "*");
    }

    public SearchCondition contains(String substring) {
        return new SearchCondition("@" + fieldName + ":*" + ValkeySyntaxUtils.escape(substring) + "*");
    }
}
