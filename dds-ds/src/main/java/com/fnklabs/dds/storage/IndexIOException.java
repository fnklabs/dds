package com.fnklabs.dds.storage;

import java.io.*;

public class IndexIOException extends IndexException {
    public IndexIOException(Exception e) {
        super(e);
    }

    public IndexIOException(String message) {
        super(message);
    }
}
