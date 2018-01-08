package com.fnklabs.dds.index;

public class IndexIOException extends IndexException {
    public IndexIOException(Exception e) {
        super(e);
    }

    public IndexIOException(String message) {
        super(message);
    }
}
