package com.fnklabs.dds.storage.im;

import com.fnklabs.dds.storage.Storage;
import com.fnklabs.dds.storage.StorageFactory;

public class ImStorageFactory implements StorageFactory {
    @Override
    public Storage get(int size) {
        return new ImStorage(size);
    }
}
