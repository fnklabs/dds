package com.fnklabs.dds.storage.columnar;

import com.fnklabs.dds.storage.Record;
import com.fnklabs.dds.storage.Reducer;
import com.fnklabs.dds.storage.StorageFactory;
import com.fnklabs.dds.storage.Table;
import com.fnklabs.dds.storage.column.Column;
import com.fnklabs.dds.storage.query.Condition;
import com.fnklabs.metrics.MetricsFactory;
import com.fnklabs.metrics.Timer;
import com.google.common.collect.Range;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public class ColumnarTable implements Table<ColumnarChunk> {
    public static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(ColumnarTable.class);
    private final String tableName;

    private final Map<Range<Long>, ColumnarChunk> chunks = new ConcurrentHashMap<>();

    private final HashFunction hashFunction = Hashing.murmur3_128();

    private final int vNodes;

    private final List<Column> columns;

    public ColumnarTable(String tableName, List<Column> columns, int vNodes, int chunkSize, StorageFactory storageFactory) {
        this.tableName = tableName;
        this.columns = columns;
        this.vNodes = vNodes;


        long step = Long.MAX_VALUE / this.vNodes * 2;

        int chunkId = 0;

        long from = Long.MIN_VALUE;

        while (from < Long.MAX_VALUE) {
            long to;

            if ((Long.MAX_VALUE - step) <= from) {
                to = Long.MAX_VALUE;
            } else {
                to = from + step;
            }

            Range<Long> tokenRange = to == Long.MAX_VALUE ? Range.closed(from, to) : Range.closedOpen(from, to);

            chunks.put(tokenRange, new ColumnarChunk(name(), columns, chunkSize, tokenRange, ++chunkId, storageFactory.get(chunkSize)));

            from = to;
        }

        LOGGER.debug("Chunks for table `{}`: {}", name(), chunks);
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
    public Set<ColumnarChunk> chunks() {
        return new HashSet<>(chunks.values());
    }

    @Override
    public void write(Record record) {
        Column primary = record.getPrimary();

        ByteBuffer buffer = ByteBuffer.allocate(primary.size());
        primary.write(record.get(primary), buffer);

        byte[] key = buffer.array();
        ColumnarChunk chunkForKey = getChunkForKey(key);

        LOGGER.debug("Write `{}` to chunk {}", key, chunkForKey);

        chunkForKey.write(record);
    }

    @Override
    public Record read(byte[] key) {
        ColumnarChunk chunkForKey = getChunkForKey(key);

        LOGGER.debug("read `{}` from chunk {}", key, chunkForKey);

        return chunkForKey.read(key);
    }

    @Override
    public <T, R> R query(String column, Condition condition, Reducer<T, R> reducer) {
        Timer timer = MetricsFactory.getMetrics().getTimer("table.columnar.query");
        Collection<T> collect = chunks().stream()
                                        .parallel()
                                        .flatMap(c -> {
                                            Collection<T> row = c.query(column, condition);

                                            return row.stream();
                                        })
                                        .collect(Collectors.toList());


        R reduce = reducer.reduce(collect);

        timer.stop();

        return reduce;
    }

    private ColumnarChunk getChunkForKey(byte[] key) {
        Set<Range<Long>> ranges = chunks.keySet();

        long token = hashFunction.hashBytes(key).asLong();


        for (Range<Long> range : ranges) {
            if (range.contains(token)) {
                ColumnarChunk columnarChunk = chunks.get(range);

                LOGGER.debug("chunk for key `{}`: {}", key, columnarChunk);

                return columnarChunk;
            }
        }

        throw new IllegalStateException("chunk for key doesn't exists");
    }
}
