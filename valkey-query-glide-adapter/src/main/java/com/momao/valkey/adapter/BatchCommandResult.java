package com.momao.valkey.adapter;

public record BatchCommandResult(
        boolean success,
        Object response,
        Exception error
) {

    static BatchCommandResult success(Object response) {
        return new BatchCommandResult(true, response, null);
    }

    static BatchCommandResult failure(Exception error) {
        return new BatchCommandResult(false, null, error);
    }
}
