package com.fnklabs.dds.storage;

import java.util.function.Consumer;

public interface Mapper<C extends Chunk, T> {
    void map(C chunk, Consumer<T> consumer);
}
