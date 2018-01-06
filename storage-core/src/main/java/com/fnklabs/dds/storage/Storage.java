package com.fnklabs.dds.storage;

import java.nio.ByteBuffer;
import java.util.function.Supplier;

public interface Storage {
    long allocatedSize();

    long actualSize();

    long items();

    /**
     * Put data
     *
     * @param position position
     * @param data     buffer from which data will read and written
     */
    void write(long position, ByteBuffer data);

    /**
     * Read data
     *
     * @param position position
     * @param data     buffer to which data will be written
     */
    void read(long position, ByteBuffer data);

    /**
     * Read data
     *
     * @param position position
     * @param buffer   buffer to which data will be written
     */
    void read(long position, byte[] buffer);

    void scan(long position, ScanFunction scanFunction, Supplier<ByteBuffer> bufferSupplier);
}
