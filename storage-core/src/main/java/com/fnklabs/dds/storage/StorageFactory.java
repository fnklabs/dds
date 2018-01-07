package com.fnklabs.dds.storage;

public interface StorageFactory<T extends StorageOptions> {
    TableStorage get(T storageOptions);
}
