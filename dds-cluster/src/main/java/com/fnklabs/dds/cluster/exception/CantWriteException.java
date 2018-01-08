package com.fnklabs.dds.cluster.exception;

public class CantWriteException extends RuntimeException {
    public CantWriteException() {
    }

    public CantWriteException(Throwable cause) {
        super(cause);
    }
}
