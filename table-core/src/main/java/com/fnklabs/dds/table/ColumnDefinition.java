package com.fnklabs.dds.table;

public interface ColumnDefinition extends Comparable<ColumnDefinition> {
    String name();

    DataType type();

    boolean isPrimary();

    int size();
}
