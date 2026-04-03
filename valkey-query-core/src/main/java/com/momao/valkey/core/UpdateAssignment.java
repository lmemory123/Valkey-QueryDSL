package com.momao.valkey.core;

public record UpdateAssignment(String fieldName, Object value) implements UpdateOperation {

    public UpdateAssignment {
        if (fieldName == null || fieldName.isBlank()) {
            throw new IllegalArgumentException("Field name cannot be blank");
        }
        if (value == null) {
            throw new IllegalArgumentException("Update value cannot be null");
        }
    }

    @Override
    public UpdateOperationKind kind() {
        return UpdateOperationKind.SET;
    }
}
