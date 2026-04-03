package com.momao.valkey.core.exception;

public class ValkeyConnectionException extends ValkeyQueryException {

    public ValkeyConnectionException(String message) {
        this(ValkeyErrorCode.CONNECTION_ERROR, message);
    }

    public ValkeyConnectionException(String message, Throwable cause) {
        this(ValkeyErrorCode.CONNECTION_ERROR, message, cause);
    }

    public ValkeyConnectionException(ValkeyErrorCode errorCode, String message) {
        super(errorCode, message);
    }

    public ValkeyConnectionException(ValkeyErrorCode errorCode, String message, Throwable cause) {
        super(errorCode, message, cause);
    }
}
