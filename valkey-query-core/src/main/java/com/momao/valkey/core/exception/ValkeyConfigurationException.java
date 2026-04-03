package com.momao.valkey.core.exception;

public class ValkeyConfigurationException extends ValkeyQueryException {

    public ValkeyConfigurationException(String message) {
        this(ValkeyErrorCode.CONFIGURATION_ERROR, message);
    }

    public ValkeyConfigurationException(String message, Throwable cause) {
        this(ValkeyErrorCode.CONFIGURATION_ERROR, message, cause);
    }

    public ValkeyConfigurationException(ValkeyErrorCode errorCode, String message) {
        super(errorCode, message);
    }

    public ValkeyConfigurationException(ValkeyErrorCode errorCode, String message, Throwable cause) {
        super(errorCode, message, cause);
    }
}
