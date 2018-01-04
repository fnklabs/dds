package com.fnklabs.dds.storage.column;

import java.nio.ByteBuffer;

public class LongColumn extends AbstractColumn<Long> {

    public LongColumn(String name, short order) {
        this(name, order, false);
    }

    public LongColumn(String name, short order, boolean isPrimary) {
        super(name, Long.class, (short) Long.BYTES, order, isPrimary);
    }

    @Override
    public Long read(ByteBuffer buffer) {
        return buffer.getLong();
    }

    @Override
    public void write(Long value, ByteBuffer buffer) {
        buffer.putLong(value);
    }
}
