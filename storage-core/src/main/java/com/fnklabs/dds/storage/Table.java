package com.fnklabs.dds.storage;

import com.fnklabs.dds.storage.column.Column;
import com.fnklabs.dds.storage.query.Condition;

import java.util.List;
import java.util.Set;

public interface Table<C extends Chunk> {
    List<Column> columns();

    /**
     * Table
     *
     * @return table name
     */
    String name();

    /**
     * Chunk from which current table is consist
     *
     * @return set of chunks
     */
    Set<C> chunks();

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
