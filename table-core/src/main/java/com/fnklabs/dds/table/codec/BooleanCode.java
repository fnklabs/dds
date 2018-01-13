package com.fnklabs.dds.table.codec;

import java.nio.ByteBuffer;

public class BooleanCode implements DataTypeCodec<Boolean> {
    @Override
    public void encode(Boolean obj, ByteBuffer buffer) {
        buffer.put((byte) (obj ? 1 : 0));
    }

    @Override
    public Boolean decode(ByteBuffer buffer) {
        byte b = buffer.get();

        return b == 1;
    }

    @Override
    public int size() {
        return 1;
    }
}
