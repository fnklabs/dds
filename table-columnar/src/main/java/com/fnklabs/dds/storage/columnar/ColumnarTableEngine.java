package com.fnklabs.dds.storage.columnar;

import com.fnklabs.dds.storage.TableStorage;
import com.fnklabs.dds.table.*;
import com.google.common.base.Verify;
import com.google.common.collect.Range;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.protobuf.ByteString;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

class ColumnarTableEngine implements TableEngine {
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(ColumnarTableEngine.class);

    private final TableDefinition tableDefinition;

    private final Map<Range<Long>, ColumnarChunk> chunks = new ConcurrentHashMap<>();

    private final ColumnDefinition primary;

    private final HashFunction hashFunction = Hashing.murmur3_32();

    private final int vNodes;
    private final long chunkSize;
    private final TableStorage tableStorage;

    ColumnarTableEngine(TableDefinition tableDefinition, TableStorage storage, int vNodes, long chunkSize) {
        this.tableDefinition = tableDefinition;
        this.tableStorage = storage;
        this.vNodes = vNodes;
        this.chunkSize = chunkSize;

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

            chunks.put(tokenRange, new ColumnarChunk(chunkId++, chunkSize, tableDefinition, tableStorage, tokenRange));

            from = to;
        }

        primary = tableDefinition.getColumnList()
                                 .stream()
                                 .filter(ColumnDefinition::getPrimary)
                                 .findFirst()
                                 .orElse(null);

        LOGGER.debug("Chunks for table `{}`: {}", tableDefinition.getName(), chunks);
    }


    @Override
    public ResultSet query(Insert insert) {
        String table = insert.getTable();

        Verify.verify(StringUtils.isNoneEmpty(table), "table name can't be empty");

        Map<String, ByteString> valueMap = insert.getValueMap();

        String primaryColumnName = primary.getName();

        Verify.verify(insert.containsValue(primaryColumnName), "primary key doesn't exists");

        byte[] primaryKeyValue = valueMap.get(primaryColumnName).toByteArray();

        ColumnarChunk chunkForKey = getChunkForKey(primaryKeyValue);

        return chunkForKey.query(insert);
    }

    @Override
    public ResultSet query(Select select) {
        return null;
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
