package com.fnklabs.dds.storage.column;

import java.nio.ByteBuffer;

public class IntegerColumn extends AbstractColumn<Integer> {

    public IntegerColumn(String name, short order, boolean isPrimary) {
        super(name, Integer.class, (short) Integer.BYTES, order, isPrimary);
    }

    public IntegerColumn(String name, short order) {
        this(name, order, false);
    }

    @Override
    public Integer read(ByteBuffer buffer) {
        return buffer.getInt();
    }

    @Override
    public void write(Integer value, ByteBuffer buffer) {
        buffer.putInt(value);
    }
}
