package com.fnklabs.dds.storage.columnar;

import com.fnklabs.dds.table.TableEngineOptions;

public class ColumnarOptions implements TableEngineOptions {
    private final int chunks;
    private final int chunkSize;

    public ColumnarOptions(int chunks, int chunkSize) {
        this.chunks = chunks;
        this.chunkSize = chunkSize;
    }

    public int getChunks() {
        return chunks;
    }

    public long getChunkSize() {
        return chunkSize;
    }
}
