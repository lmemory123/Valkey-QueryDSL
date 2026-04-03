package com.momao.valkey.core.exception;

public class ValkeyQueryExecutionException extends ValkeyQueryException {

    public ValkeyQueryExecutionException(String message) {
        this(ValkeyErrorCode.QUERY_EXECUTION_ERROR, message);
    }

    public ValkeyQueryExecutionException(String message, Throwable cause) {
        this(ValkeyErrorCode.QUERY_EXECUTION_ERROR, message, cause);
    }

    public ValkeyQueryExecutionException(ValkeyErrorCode errorCode, String message) {
        super(errorCode, message);
    }

    public ValkeyQueryExecutionException(ValkeyErrorCode errorCode, String message, Throwable cause) {
        super(errorCode, message, cause);
    }
}
