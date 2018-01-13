package com.fnklabs.dds.table.codec;

import java.nio.ByteBuffer;

public class DoubleCodec implements DataTypeCodec<Double> {
    @Override
    public void encode(Double obj, ByteBuffer buffer) {
        buffer.putDouble(obj);
    }

    @Override
    public Double decode(ByteBuffer buffer) {
        return buffer.getDouble();
    }

    @Override
    public int size() {
        return Double.BYTES;
    }
}
