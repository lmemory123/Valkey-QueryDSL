package com.momao.valkey.core.exception;

public class ValkeyIndexException extends ValkeyQueryException {

    public ValkeyIndexException(String message) {
        this(ValkeyErrorCode.INDEX_ERROR, message);
    }

    public ValkeyIndexException(String message, Throwable cause) {
        this(ValkeyErrorCode.INDEX_ERROR, message, cause);
    }

    public ValkeyIndexException(ValkeyErrorCode errorCode, String message) {
        super(errorCode, message);
    }

    public ValkeyIndexException(ValkeyErrorCode errorCode, String message, Throwable cause) {
        super(errorCode, message, cause);
    }
}
