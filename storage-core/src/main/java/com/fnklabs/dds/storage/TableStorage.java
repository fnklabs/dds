package com.fnklabs.dds.storage;

import java.util.function.Supplier;

/**
 * Interface for table storage implementation
 */
public interface TableStorage {
    /**
     * Total allocated table size
     *
     * @return table size
     */
    long allocatedSize();

    /**
     * Put data
     *
     * @param position position
     * @param data     buffer from which data will read and written
     */
    void write(long position, byte[] data);

    /**
     * Read data
     *
     * @param position position
     * @param data     buffer to which data will be written
     */
    void read(long position, byte[] data);


    void scan(long position, ScanFunction scanFunction, Supplier<byte[]> bufferSupplier);
}
