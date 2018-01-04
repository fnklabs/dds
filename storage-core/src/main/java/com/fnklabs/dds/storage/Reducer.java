package com.fnklabs.dds.storage;

import java.util.Collection;

public interface Reducer<T, R> {
    R reduce(Collection<T> items);

}
