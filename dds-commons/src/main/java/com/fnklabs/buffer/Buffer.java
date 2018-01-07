package com.fnklabs.buffer;

public interface Buffer {

    long bufferSize();

    int read(long position, byte[] data);

    /**
     * @param position buffer position
     * @param data     dst buffer
     * @param offset   dst offset
     * @param length   dst length
     *
     * @return
     */
    int read(long position, byte[] data, long offset, int length);

    void write(long position, byte[] data);

    void write(long position, byte[] data, long offset, int length);
}
