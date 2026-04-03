package com.momao.valkey.core;

public sealed interface UpdateOperation permits UpdateAssignment, NumericUpdateOperation {

    String fieldName();

    UpdateOperationKind kind();
}
