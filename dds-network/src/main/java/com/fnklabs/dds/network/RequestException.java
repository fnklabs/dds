package com.fnklabs.dds.network;

public class RequestException extends RuntimeException {
    private StatusCode statusCode = StatusCode.UNKNOWN;

    public RequestException() {
    }

    public RequestException(StatusCode statusCode) {
        this.statusCode = statusCode;
    }

    public RequestException(Throwable cause) {
        super(cause);
    }

    public RequestException(String message) {
        super(message);
    }

    public StatusCode getStatusCode() {
        return statusCode;
    }
}
