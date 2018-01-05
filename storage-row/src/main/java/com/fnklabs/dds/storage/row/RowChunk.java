package com.fnklabs.dds.storage.row;

import com.fnklabs.dds.storage.Chunk;
import com.fnklabs.dds.storage.Record;
import com.fnklabs.dds.storage.Storage;
import com.fnklabs.dds.storage.column.Column;
import com.fnklabs.dds.storage.query.Condition;
import com.fnklabs.metrics.Counter;
import com.fnklabs.metrics.MetricsFactory;
import com.fnklabs.metrics.Timer;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class RowChunk implements Chunk {
    public static final Logger LOGGER = LoggerFactory.getLogger(RowChunk.class);

    private final String tableName;
    private final int id;

    private final Set<Column> columns;

    private final Column primaryColumn;
    private final int rowSize;

    private final Storage storage;


    private final Counter sizeCounter;
    private final Counter itemsCounter;

    private final AtomicInteger position = new AtomicInteger(0);

    public RowChunk(String tableName, List<Column> columns, int id, Storage storage) {
        this.tableName = tableName;
        this.columns = new TreeSet<>(columns);
        this.id = id;

        LOGGER.debug("columns: {}", this.columns);

        this.rowSize = columns.stream().map(c -> (int) c.size()).reduce(Integer::sum).orElse(0);

        this.storage = storage;
        this.primaryColumn = columns.stream().filter(Column::isPrimary).findFirst().orElse(null);

        this.sizeCounter = MetricsFactory.getMetrics().getCounter(String.format("chunk.%s-%d.size", name(), id));
        this.itemsCounter = MetricsFactory.getMetrics().getCounter(String.format("chunk.%s-%d.items", name(), id));
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
        return 0;
    }

    @Override
    public int maxSize() {
        return 0;
    }

    @Override
    public void write(Record record) {
        itemsCounter.inc();

        int position = this.position.getAndAdd(rowSize);

        ByteBuffer buffer = ByteBuffer.allocate(rowSize);

        for (Column column : columns) {
            record.get(column, buffer);
        }

        buffer.rewind();

        storage.write(position, buffer);
    }

    @Override
    public Record read(byte[] key) {
        int currentPosition = position.get();

        ByteBuffer buffer = ByteBuffer.allocate(rowSize);

        for (int i = 0; i < currentPosition; i = i + rowSize) {
            storage.read(i, buffer);

            buffer.rewind();

            byte[] data = new byte[primaryColumn.size()];

            buffer.get(data);

            if (Arrays.equals(key, data)) {
                buffer.rewind();

                return getRecord(buffer);
            }

            buffer.clear();
        }

        return null;
    }

    @Override
    public <T> Collection<T> query(String column, Condition condition) {
        Timer timer = MetricsFactory.getMetrics().getTimer("chunk.row.query");

        ByteBuffer buffer = ByteBuffer.allocate(rowSize);

        Collection<T> result = new ArrayList<>();

        int position = 0;

        int currentPositionEnd = this.position.get();

        while (position < currentPositionEnd) {

            storage.read(position, buffer);

            buffer.rewind();

            Object primaryVal = primaryColumn.read(buffer);

            if (condition.test(primaryVal)) {
                buffer.rewind();

                Record record = getRecord(buffer);

                result.add(record.get(column));
            }

            buffer.clear();

            int newPosition = position + rowSize;

            if (newPosition > currentPositionEnd) {
                newPosition = currentPositionEnd;
            }

            position = newPosition;
        }


        timer.stop();

        return result;
    }

    @NotNull
    private Record getRecord(ByteBuffer buffer) {
        Map<Column, Object> columns = this.columns.stream()
                                                  .collect(Collectors.toMap(
                                                          c -> c,
                                                          c -> c.read(buffer),
                                                          (a, b) -> a
                                                  ));

        return new Record(columns);
    }

    private Column getColumn(String columnName) {
        return columns.stream()
                      .filter(c -> StringUtils.equals(columnName, c.name()))
                      .findFirst()
                      .orElse(null);
    }
}
