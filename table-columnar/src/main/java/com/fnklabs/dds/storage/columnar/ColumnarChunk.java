package com.fnklabs.dds.storage.columnar;

import com.fnklabs.dds.storage.TableStorage;
import com.fnklabs.dds.table.ColumnDefinition;
import com.fnklabs.dds.table.ResultSet;
import com.fnklabs.dds.table.TableDefinition;
import com.fnklabs.dds.table.TableEngine;
import com.fnklabs.dds.table.query.Query;
import com.fnklabs.metrics.Counter;
import com.fnklabs.metrics.MetricsFactory;
import com.google.common.collect.Range;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

public class ColumnarChunk implements TableEngine {
    private static final Logger LOGGER = LoggerFactory.getLogger(ColumnarChunk.class);

    private final int id;
    private final TableDefinition tableDefinition;

    private final Range<Long> tokenRange;


    private final AtomicLong position = new AtomicLong();

    private final Map<ColumnDefinition, Integer> columnOffset = new ConcurrentHashMap<>();

    private final ColumnDefinition primaryColumnDefinition;

    private final TableStorage storage;

    private final Counter sizeCounter;
    private final Counter itemsCounter;

    private final int primaryKeySize;
    private final int rowSize;
    private final long maxSize;

    public ColumnarChunk(int id, long maxSize, TableDefinition tableDefinition, TableStorage storage, Range<Long> tokenRange) {
        this.id = id;
        this.maxSize = maxSize;
        this.tableDefinition = tableDefinition;
        this.storage = storage;
        this.tokenRange = tokenRange;

        primaryColumnDefinition = tableDefinition.columns()
                                                 .stream()
                                                 .filter(ColumnDefinition::isPrimary)
                                                 .findFirst()
                                                 .orElse(null);

        primaryKeySize = primaryColumnDefinition.size();


        rowSize = tableDefinition.columns()
                                 .stream()
                                 .mapToInt(c -> c.isPrimary() ? c.size() : c.size() + primaryKeySize)
                                 .sum();

        int offset = 0;

        LOGGER.debug("row size {}/{}", primaryKeySize, rowSize);

        for (ColumnDefinition columnDefinition : tableDefinition.columns()) {
            columnOffset.put(columnDefinition, offset);


            int columnBlockSize = columnDefinition.size() + (columnDefinition.isPrimary() ? 0 : primaryKeySize);
            long requiredColumnBlockSize = (long) columnBlockSize * maxSize / rowSize;

            LOGGER.debug("column `{}` block size: {} total block size: {}", columnDefinition.name(), columnBlockSize, requiredColumnBlockSize);

            offset += requiredColumnBlockSize;
        }

        LOGGER.debug("columns: {} blocks: {}", tableDefinition.columns(), offset);


        sizeCounter = MetricsFactory.getMetrics().getCounter(String.format("chunk.%s-%d.size", tableDefinition.name(), id));
        itemsCounter = MetricsFactory.getMetrics().getCounter(String.format("chunk.%s-%d.items", tableDefinition.name(), id));
    }


    @Override
    public ResultSet query(Query query) {
        return null;
    }
}
