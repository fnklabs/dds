package com.fnklabs.dds.network;

public class TimeoutException extends RequestException {
    private final RequestMessage message;


    public TimeoutException(RequestMessage message) {
        super(String.format("timeout for request: %s", message.getId()));
        this.message = message;
    }

    public TimeoutException(RequestMessage message, long latency) {
        super(String.format("timeout `%d` for request: %s", latency, message.getId()));
        this.message = message;
    }
}
