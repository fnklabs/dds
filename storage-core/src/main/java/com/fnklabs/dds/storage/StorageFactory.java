package com.fnklabs.dds.storage;

public interface StorageFactory {
    Storage get(int size);
}
