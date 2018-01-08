package com.fnklabs.dds.index;

import java.io.Closeable;
import java.util.*;

public interface Index extends Closeable {
    /**
     * Get node from node
     *
     * @param key Index key
     *
     * @return Node if present
     */
    Optional<Long> get(byte[] key);

    /**
     * Add node to index
     *
     * @param key          Node
     * @param dataPosition dataPosition
     *
     * @return Node if present
     */
    boolean put(byte[] key, long dataPosition);

    /**
     * Count of entries in index
     *
     * @return entries count
     */
    long length() throws IndexIOException;

    /**
     * Size of index in bytes
     *
     * @return bytes
     */
    long size() throws IndexIOException;
}
