package com.fnklabs.dds.storage.im;

import com.fnklabs.dds.storage.StorageOptions;

public class ImStorageOptions implements StorageOptions {
    private final long maxSize;
    private final int bufferSize;
    private int chunkSize;

    public ImStorageOptions(long maxSize, int bufferSize) {
        this.maxSize = maxSize;
        this.bufferSize = bufferSize;
    }

    public long maxSize() {
        return maxSize;
    }

    public int bufferSize() {
        return bufferSize;
    }

    public int chunkSize() {
        return chunkSize;
    }
}
