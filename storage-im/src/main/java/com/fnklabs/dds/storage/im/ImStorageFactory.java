package com.fnklabs.dds.storage.im;

import com.fnklabs.dds.storage.StorageFactory;
import com.fnklabs.dds.storage.TableStorage;

public class ImStorageFactory implements StorageFactory<ImStorageOptions> {

    @Override
    public TableStorage get(ImStorageOptions storageOptions) {
        return new ImTableStorage(storageOptions.maxSize());
    }
}
