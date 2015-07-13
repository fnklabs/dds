package com.fnklabs.dds.network.exception;

import com.fnklabs.dds.network.StatusCode;

public class RequestException extends RuntimeException {
    private StatusCode statusCode = StatusCode.UNKNOWN;

    public RequestException(StatusCode statusCode) {
        this.statusCode = statusCode;
    }

    public RequestException() {
    }

    public RequestException(Throwable cause) {
        super(cause);
    }

    public StatusCode getStatusCode() {
        return statusCode;
    }
}
