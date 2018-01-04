package com.fnklabs.dds.storage.column;

import java.nio.ByteBuffer;

public interface Column<T> extends Comparable<Column<T>> {
    String name();

    Class<T> type();

    short size();

    short order();

    T read(ByteBuffer buffer);

    void write(T value, ByteBuffer buffer);

    boolean isPrimary();

    @Override
    default int compareTo(Column<T> o) {
        return Short.compare(order(), o.order());
    }

}
