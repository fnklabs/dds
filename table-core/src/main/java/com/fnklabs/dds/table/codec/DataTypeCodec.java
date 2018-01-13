package com.fnklabs.dds.table.codec;

import java.nio.ByteBuffer;

public interface DataTypeCodec<T> {
    void encode(T obj, ByteBuffer buffer);

    T decode(ByteBuffer buffer);

    int size();
}
