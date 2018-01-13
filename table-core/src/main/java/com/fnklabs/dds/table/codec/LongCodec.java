package com.fnklabs.dds.table.codec;

import java.nio.ByteBuffer;

public class LongCodec implements DataTypeCodec<Long> {
    @Override
    public void encode(Long obj, ByteBuffer buffer) {
        buffer.putLong(obj);
    }

    @Override
    public Long decode(ByteBuffer buffer) {
        return buffer.getLong();
    }

    @Override
    public int size() {
        return Long.BYTES;
    }
}
