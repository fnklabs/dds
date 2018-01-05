package com.fnklabs.dds.storage;

import java.nio.ByteBuffer;
import java.util.function.Supplier;

public interface Storage {
    int allocatedSize();

    int actualSize();

    int items();

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
     * @param data     buffer to which data will be written
     */
    void read(int position, ByteBuffer data);

    /**
     * Read data
     *
     * @param position position
     * @param buffer   buffer to which data will be written
     */
    void read(int position, byte[] buffer);

    void scan(int position, ScanFunction scanFunction, Supplier<ByteBuffer> bufferSupplier);
}
