package com.fnklabs.dds.storage.columnar;

import com.fnklabs.dds.storage.Chunk;
import com.fnklabs.dds.storage.Record;
import com.fnklabs.dds.storage.Storage;
import com.fnklabs.dds.storage.column.Column;
import com.fnklabs.dds.storage.query.Condition;
import com.fnklabs.metrics.Counter;
import com.fnklabs.metrics.MetricsFactory;
import com.fnklabs.metrics.Timer;
import com.google.common.base.MoreObjects;
import com.google.common.collect.Range;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

public class ColumnarChunk implements Chunk {
    public static final Logger LOGGER = LoggerFactory.getLogger(ColumnarChunk.class);
    private final String tableName;
    private final int id;
    private final int maxSize;

    private final Range<Long> tokenRange;

    private final Set<Column> columns;

    private final Map<Column, Integer> columnOffset = new ConcurrentHashMap<>();
    private final Map<Column, AtomicInteger> columnPositions = new ConcurrentHashMap<>();
    private final Column primaryColumn;

    private final Storage storage;

    private final short primaryColumnSize;
    private final ReentrantLock lock = new ReentrantLock();

    private final Counter sizeCounter;
    private final Counter itemsCounter;

    public ColumnarChunk(String tableName, List<Column> columns, int maxSize, Range<Long> tokenRange, int id, Storage storage) {
        this.tableName = tableName;
        this.columns = new TreeSet<>(columns);
        this.id = id;
        this.maxSize = maxSize;
        this.tokenRange = tokenRange;

        primaryColumnSize = columns.stream()
                                   .filter(Column::isPrimary)
                                   .findFirst()
                                   .map(Column::size)
                                   .orElse(Short.MAX_VALUE);

        int columnsSize = columns.stream()
                                 .mapToInt(c -> c.size() + (c.isPrimary() ? 0 : primaryColumnSize))
                                 .sum();

//        short totalColumnsSize = Integer.valueOf(columnsSize).shortValue();

        LOGGER.debug("primary size: {} total columns size: {}", primaryColumnSize, columnsSize);

        int offset = 0;

        for (Column column : columns) {
            columnOffset.put(column, offset);
            columnPositions.put(column, new AtomicInteger(offset));

            int columnBlockSize = column.size() + Integer.valueOf(column.isPrimary() ? 0 : primaryColumnSize);
            Long requiredColumnBlockSize = (long) columnBlockSize * (long) maxSize / columnsSize;

            LOGGER.debug("column `{}` block size: {} total block size: {}", column, columnBlockSize, requiredColumnBlockSize);

            offset += requiredColumnBlockSize;
        }

        LOGGER.debug("columns: {}", this.columns);
        LOGGER.debug("columns blocks: {}", columnPositions);

        this.storage = storage;
        primaryColumn = columns.stream().filter(Column::isPrimary).findFirst().orElse(null);

        sizeCounter = MetricsFactory.getMetrics().getCounter(String.format("chunk.%s-%d.size", name(), id));
        itemsCounter = MetricsFactory.getMetrics().getCounter(String.format("chunk.%s-%d.items", name(), id));
    }

    @Override
    public String name() {
        return tableName;
    }

    @Override
    public int id() {
        return id;
    }

    @Override
    public int size() {
        return maxSize;
    }

    @Override
    public int maxSize() {
        return maxSize;
    }

    @Override
    public void write(Record record) {
        lock.lock();

        try {
            itemsCounter.inc();

            ByteBuffer buffer = ByteBuffer.allocate(4 * 1024);

            for (Column column : columns) {
                int delta = column.size() + (column.isPrimary() ? 0 : primaryColumnSize);

                sizeCounter.inc(delta);

                LOGGER.debug("delta: {}", delta);

                int currentPosition = columnPositions.get(column).getAndAdd(delta);


                record.get(column, buffer);

                if (!column.isPrimary()) {
                    Column primaryColumn = record.getPrimary();

                    record.get(primaryColumn, buffer);
                }

                buffer.flip();

                storage.write(currentPosition, buffer);

                buffer.clear();
            }
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Record read(byte[] key) {
        int currentPosition = columnPositions.get(primaryColumn).get();

        ByteBuffer buffer = ByteBuffer.allocate(4 * 1024);

        int block = -1;

        for (int i = 0; i < currentPosition; i = i + primaryColumnSize) {
            storage.read(i, primaryColumnSize, buffer);

            buffer.rewind();

            byte[] data = new byte[primaryColumnSize];

            buffer.get(data);

            if (Arrays.equals(key, data)) {
                block = i / primaryColumnSize;
                break;
            }

            buffer.clear();
        }

        buffer.clear();

        if (block != -1) {
            int finalBlock = block;
            Map<Column, Object> data = columns.stream()
                                              .collect(Collectors.toMap(
                                                      c -> c,
                                                      c -> {
                                                          readColumnBlock(c, finalBlock, buffer);

                                                          buffer.flip();

                                                          Object value = c.read(buffer);

                                                          buffer.clear();

                                                          return value;
                                                      },
                                                      (a, b) -> a
                                              ));

            return new Record(data);

        }

        return null;
    }

    @Override
    public <T> Collection<T> query(String columnName, Condition condition) {
        Timer timer = MetricsFactory.getMetrics().getTimer("chunk.columnar.query");

        Column column = getColumn(columnName);

        Integer offset = columnOffset.get(column);
        int currentSize = columnPositions.get(column).get();

        int columnBlockSize = column.size() + (column.isPrimary() ? 0 : primaryColumnSize);

        ByteBuffer buffer = ByteBuffer.allocate(columnBlockSize);

        Collection<T> result = new ArrayList<>();

        for (int position = offset; position < currentSize; position = position + columnBlockSize) {
            storage.read(position, columnBlockSize, buffer);

            buffer.rewind();
            buffer.position(column.size());

            Object primaryVal = primaryColumn.read(buffer);

            if (condition.test(primaryVal)) {
                buffer.rewind();

                Object columnValue = column.read(buffer);

                result.add((T) columnValue);
            }

            buffer.rewind();
        }

        timer.stop();

        return result;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                          .add("table", tableName)
                          .add("id", id())
                          .add("range", tokenRange)
                          .toString();
    }

    private void readColumnBlock(Column c, int index, ByteBuffer buffer) {
        int blockSize = c.size() + (c.isPrimary() ? 0 : primaryColumnSize);

        int blockPosition = columnOffset.get(c) + index * blockSize;

        storage.read(blockPosition, blockSize, buffer);
    }

    private Object readColumnValue(Column c, int index, ByteBuffer buffer) {
        int blockSize = c.size() + (c.isPrimary() ? 0 : primaryColumnSize);

        int blockPosition = columnOffset.get(c) + index * blockSize;

        storage.read(blockPosition, blockSize, buffer);

        buffer.flip();

        return c.read(buffer);
    }

    private Column getColumn(String columnName) {
        return columns.stream()
                      .filter(c -> StringUtils.equals(columnName, c.name()))
                      .findFirst()
                      .orElse(null);
    }
}
