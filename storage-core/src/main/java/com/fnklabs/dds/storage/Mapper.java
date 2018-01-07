package com.fnklabs.dds.storage;

import java.util.function.Consumer;

public interface Mapper<T extends TableStorage, O> {
    void map(T chunk, Consumer<O> consumer);
}
