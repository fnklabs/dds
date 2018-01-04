package com.fnklabs.dds.storage;

import java.util.Collection;

public interface Task<C extends Chunk, R> {
    R map(Collection<C> chunkCollection);
}
