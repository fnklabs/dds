package com.fnklabs.dds.table.codec;

import com.fnklabs.dds.BytesUtils;

import java.nio.ByteBuffer;

public interface DataTypeCodec<T> {
    void encode(T obj, ByteBuffer buffer);

    T decode(ByteBuffer buffer);

    int size();

    default int compare(byte[] a, byte[] b) {
        return BytesUtils.compare(a, b);
    }
}
