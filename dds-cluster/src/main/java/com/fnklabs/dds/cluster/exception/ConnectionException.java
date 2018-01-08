package com.fnklabs.dds.cluster.exception;

import com.google.common.net.HostAndPort;

public class ConnectionException extends RuntimeException {
    private HostAndPort address;

    public ConnectionException(String message, HostAndPort address) {
        super(message);
        this.address = address;
    }

    public ConnectionException(String message, Throwable cause, HostAndPort address) {
        super(message, cause);
        this.address = address;
    }
}
