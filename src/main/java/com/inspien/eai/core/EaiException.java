package com.inspien.eai.core;

public class EaiException extends RuntimeException {

    private final ErrorCode code;

    public EaiException(ErrorCode code, String message) {
        this(code, message, null);
    }

    public EaiException(ErrorCode code, String message, Throwable cause) {
        super(message, cause);
        this.code = code;
    }

    public ErrorCode code() { return code; }
}
