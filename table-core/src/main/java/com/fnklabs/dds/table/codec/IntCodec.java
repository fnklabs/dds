package com.fnklabs.dds.table.codec;

import java.nio.ByteBuffer;

public class IntCodec implements DataTypeCodec<Integer> {

    @Override
    public void encode(Integer obj, ByteBuffer buffer) {
        buffer.putInt(obj);
    }

    @Override
    public Integer decode(ByteBuffer buffer) {
        return buffer.getInt();
    }

    @Override
    public int size() {
        return Integer.BYTES;
    }
}
