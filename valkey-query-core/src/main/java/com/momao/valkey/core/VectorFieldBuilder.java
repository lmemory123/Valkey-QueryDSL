package com.momao.valkey.core;

public final class VectorFieldBuilder implements ValkeyFieldReference {

    private final String fieldName;

    public VectorFieldBuilder(String fieldName) {
        this.fieldName = fieldName;
    }

    @Override
    public String fieldName() {
        return fieldName;
    }
}
