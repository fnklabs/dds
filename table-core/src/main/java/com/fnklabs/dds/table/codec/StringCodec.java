package com.fnklabs.dds.table.codec;

import java.nio.ByteBuffer;

public class StringCodec implements DataTypeCodec<String> {

    @Override
    public void encode(String obj, ByteBuffer buffer) {
        byte[] bytes = obj.getBytes();

        buffer.putInt(bytes.length);
        buffer.put(bytes);
    }

    @Override
    public String decode(ByteBuffer buffer) {
        int strLength = buffer.getInt();

        byte[] str = new byte[strLength];

        buffer.get(str);

        return new String(str);
    }

    @Override
    public int size() {
        return -1;
    }
}
