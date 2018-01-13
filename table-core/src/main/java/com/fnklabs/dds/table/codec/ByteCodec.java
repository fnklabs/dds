package com.fnklabs.dds.table.codec;

import java.nio.ByteBuffer;

public class ByteCodec implements DataTypeCodec<Byte> {

    @Override
    public void encode(Byte obj, ByteBuffer buffer) {
        buffer.put(obj);
    }

    @Override
    public Byte decode(ByteBuffer buffer) {
        return buffer.get();
    }

    @Override
    public int size() {
        return 1;
    }
}
