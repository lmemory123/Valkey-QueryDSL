package com.momao.valkey.core.exception;

public class ValkeyResultMappingException extends ValkeyQueryException {

    public ValkeyResultMappingException(String message) {
        this(ValkeyErrorCode.RESULT_MAPPING_ERROR, message);
    }

    public ValkeyResultMappingException(String message, Throwable cause) {
        this(ValkeyErrorCode.RESULT_MAPPING_ERROR, message, cause);
    }

    public ValkeyResultMappingException(ValkeyErrorCode errorCode, String message) {
        super(errorCode, message);
    }

    public ValkeyResultMappingException(ValkeyErrorCode errorCode, String message, Throwable cause) {
        super(errorCode, message, cause);
    }
}
