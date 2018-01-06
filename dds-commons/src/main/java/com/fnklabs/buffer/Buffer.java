package com.fnklabs.buffer;

public interface Buffer {
    long allocatedSize();

    long bufferSize();

    int read(long position, byte[] data);

    void write(long position, byte[] data);
}
