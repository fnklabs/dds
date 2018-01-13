package com.fnklabs.dds.storage.row;

import com.fnklabs.dds.storage.Record;
import com.fnklabs.dds.storage.Reducer;
import com.fnklabs.dds.storage.StorageFactory;
import com.fnklabs.dds.storage.Table;
import com.fnklabs.dds.storage.column.Column;
import com.fnklabs.dds.table.query.Condition;
import com.fnklabs.metrics.MetricsFactory;
import com.fnklabs.metrics.Timer;
import com.google.common.collect.Sets;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Set;

public class RowTable implements Table<RowChunk> {
    public static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(RowTable.class);

    private final String tableName;

    private final List<Column> columns;

    private final RowChunk chunk;

    public RowTable(String tableName, List<Column> columns, int size, StorageFactory storageFactory) {
        this.tableName = tableName;
        this.columns = columns;
        chunk = new RowChunk(tableName, columns, 0, storageFactory.get(size));
    }

    @Override
    public List<Column> columns() {
        return columns;
    }

    @Override
    public String name() {
        return tableName;
    }

    @Override
    public Set<RowChunk> chunks() {
        return Sets.newHashSet(chunk);
    }

    @Override
    public void write(Record record) {
        chunk.write(record);
    }

    @Override
    public Record read(byte[] key) {
        return chunk.read(key);
    }

    @Override
    public <T, R> R query(String column, Condition condition, Reducer<T, R> reducer) {
        Timer timer = MetricsFactory.getMetrics().getTimer("table.row.query");

        Collection<T> collect = chunk.query(column, condition);

        R reduce = reducer.reduce(collect);

        timer.stop();

        return reduce;
    }
}
