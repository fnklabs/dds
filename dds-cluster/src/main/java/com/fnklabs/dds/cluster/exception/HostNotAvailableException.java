package com.fnklabs.dds.cluster.exception;

import com.google.common.net.HostAndPort;

public class HostNotAvailableException extends ConnectionException {


    public HostNotAvailableException(String message, HostAndPort address) {
        super(message, address);
    }

    public HostNotAvailableException(String message, Throwable cause, HostAndPort address) {
        super(message, cause, address);
    }
}
