package com.fnklabs.dds.storage;

import com.fnklabs.dds.storage.query.Condition;

import java.util.Collection;

public interface Chunk {
    /**
     * Table name
     *
     * @return name of storage
     */
    String name();

    /**
     * Return chunk id
     *
     * @return int
     */
    int id();

    /**
     * Return allocated chunk size
     *
     * @return chunk size
     */
    int size();

    /**
     * Return maximum chunk size
     *
     * @return max chunk size
     */
    int maxSize();

    /**
     * Put data to chunk
     * @param record     buffer from which data will read and written*/
    void write(Record record);

    /**
     * Read data from chunk
     *  @param key chunk position
     * */
    Record read(byte[] key);

    <T> Collection<T> query(String column, Condition condition);
}
