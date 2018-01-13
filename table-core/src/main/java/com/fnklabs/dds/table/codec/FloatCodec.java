package com.fnklabs.dds.table.codec;

import java.nio.ByteBuffer;

public class FloatCodec implements DataTypeCodec<Float> {
    @Override
    public void encode(Float obj, ByteBuffer buffer) {
        buffer.putFloat(obj);
    }

    @Override
    public Float decode(ByteBuffer buffer) {
        return buffer.getFloat();
    }

    @Override
    public int size() {
        return Float.BYTES;
    }
}
