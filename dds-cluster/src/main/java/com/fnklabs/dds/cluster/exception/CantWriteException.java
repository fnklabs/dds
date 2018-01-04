package com.fnklabs.dds.coordinator.exception;

public class CantWriteException extends RuntimeException {
    public CantWriteException() {
    }

    public CantWriteException(Throwable cause) {
        super(cause);
    }
}
