package com.momao.valkey.core.exception;

public class ValkeyQueryException extends RuntimeException {

    private final ValkeyErrorCode errorCode;

    public ValkeyQueryException(ValkeyErrorCode errorCode, String message) {
        super(message);
        this.errorCode = errorCode;
    }

    public ValkeyQueryException(ValkeyErrorCode errorCode, String message, Throwable cause) {
        super(message, cause);
        this.errorCode = errorCode;
    }

    public ValkeyErrorCode errorCode() {
        return errorCode;
    }

    public String errorCategory() {
        return errorCode.category();
    }

    public String errorCodeValue() {
        return errorCode.code();
    }
}
