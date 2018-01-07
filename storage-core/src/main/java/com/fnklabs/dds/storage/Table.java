package com.fnklabs.dds.storage;

import com.fnklabs.dds.storage.column.Column;
import com.fnklabs.dds.storage.query.Condition;

import java.util.List;

public interface Table {
    List<Column> columns();

    /**
     * Table
     *
     * @return table name
     */
    String name();

    /**
     * Write data into table
     *
     * @param record
     *
     * @return
     */
    void write(Record record);

    /**
     * Read data from table
     *
     * @param key
     *
     * @return data or null is not present
     */
    Record read(byte[] key);

    <T, R> R query(String column, Condition condition, Reducer<T, R> reducer);
}
