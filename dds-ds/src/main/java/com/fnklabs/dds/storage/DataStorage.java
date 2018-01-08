package com.fnklabs.dds.storage;

import org.jetbrains.annotations.Nullable;

import java.io.IOException;
import java.util.function.BiFunction;
import java.util.function.BiPredicate;

/**
 * Data storage interface
 */
public interface DataStorage {
    /**
     * Get DataBlock by provided key.
     * <p>
     * Current method will scan all storage (via {@link #scan(BiPredicate, BiFunction)}) and filter DataBlock by key predicate
     *
     * @param dataKey key
     *
     * @return DataBlock
     */
    @Nullable
    DataBlock get(byte[] dataKey);

    /**
     * Try to read data block directly from provided position
     *
     * @param position DataBlock position
     *
     * @return DataPosition
     */
    @Nullable
    DataBlock get(long position) throws IOException;

    /**
     * Write or update DataBlock by key if entry is already exists
     *
     * @param dataKey Key
     * @param data    Value
     *
     * @return DataBlock position
     *
     * @throws IOException if can't perform R/W operation
     */
    long put(byte[] dataKey, byte[] data) throws IOException;

    /**
     * Scan all DataStorage sequentially provide entry to predicate and provide DataBlock to consumer
     *
     * @param predicate DataBlock filter
     * @param consumer  DataBlock consumer called if predicate return true. If consumer return true than scan will be stopped
     *
     * @return Number of successfully tested entries
     */
    long scan(BiPredicate<Long, DataBlock> predicate, BiFunction<Long, DataBlock, Boolean> consumer);
}
