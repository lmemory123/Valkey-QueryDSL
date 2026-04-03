package com.momao.valkey.core;

import java.util.Arrays;
import java.util.stream.Collectors;

public final class TagFieldBuilder implements ValkeyFieldReference {

    private final String fieldName;

    public TagFieldBuilder(String fieldName) {
        this.fieldName = fieldName;
    }

    @Override
    public String fieldName() {
        return fieldName;
    }

    public SearchCondition eq(String value) {
        return SearchCondition.tagEquals(fieldName, value);
    }

    public SearchCondition contains(String value) {
        return eq(value);
    }

    public SearchCondition in(String... values) {
        if (values == null || values.length == 0) {
            throw new IllegalArgumentException("Values cannot be empty");
        }
        String joined = Arrays.stream(values)
                .map(ValkeySyntaxUtils::escapeTag)
                .collect(Collectors.joining(" | "));
        return new SearchCondition("@" + fieldName + ":{" + joined + "}");
    }
}
