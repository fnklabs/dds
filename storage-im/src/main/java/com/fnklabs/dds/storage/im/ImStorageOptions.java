package com.fnklabs.dds.storage.im;

import com.fnklabs.dds.storage.StorageOptions;

public class ImStorageOptions implements StorageOptions{
    private final long maxSize;

    public ImStorageOptions(long maxSize) {this.maxSize = maxSize;}

    public long maxSize() {
        return maxSize;
    }
}
