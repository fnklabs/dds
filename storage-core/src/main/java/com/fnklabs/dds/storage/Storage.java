package com.fnklabs.dds.storage;

import java.nio.ByteBuffer;

public interface Storage {
    /**
     * Put data
     *
     * @param position position
     * @param data     buffer from which data will read and written
     */
    void write(int position, ByteBuffer data);

    /**
     * Read data
     *
     * @param position position
     * @param length   length
     * @param data     buffer to which data will be written
     */
    void read(int position, int length, ByteBuffer data);
}
