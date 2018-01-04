package com.fnklabs.dds.storage.column;

import java.nio.ByteBuffer;

public class StringColumn extends AbstractColumn<String> {

    public StringColumn(String name, short size, short order, boolean isPrimary) {
        super(name, String.class, size, order, isPrimary);
    }

    @Override
    public String read(ByteBuffer buffer) {
        byte[] arr = new byte[size()];

        buffer.get(arr);

        return new String(arr);
    }

    @Override
    public void write(String value, ByteBuffer buffer) {
        if (value.length() > size()) {
            value = value.substring(0, size());
        }

        buffer.put(value.getBytes());
    }
}
