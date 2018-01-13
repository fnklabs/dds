package com.fnklabs.dds.storage.columnar;

import com.fnklabs.dds.storage.TableStorage;
import com.fnklabs.dds.table.ResultSet;
import com.fnklabs.dds.table.TableDefinition;
import com.fnklabs.dds.table.TableEngine;
import com.fnklabs.dds.table.query.Query;
import com.google.common.collect.Range;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public class ColumnarTableEngine implements TableEngine {
    private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(ColumnarTableEngine.class);

    private final TableDefinition tableDefinition;

    private final Map<Range<Long>, ColumnarChunk> chunks = new ConcurrentHashMap<>();

    private final HashFunction hashFunction = Hashing.murmur3_128();

    private final int vNodes;
    private final long chunkSize;
    private final TableStorage tableStorage;

    public ColumnarTableEngine(TableDefinition tableDefinition, TableStorage storage, int vNodes, long chunkSize) {
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

        LOGGER.debug("Chunks for table `{}`: {}", tableDefinition.name(), chunks);
    }


    @Override
    public ResultSet query(Query query) {
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
