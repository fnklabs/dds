package com.fnklabs.dds.cluster;

import com.google.common.collect.Range;
import org.jetbrains.annotations.NotNull;

public interface Partition extends Comparable<Partition> {
    Range<Long> range();

    @Override
    default int compareTo(@NotNull Partition o) {
        Long.compare(range().lowerEndpoint(), o.range().lowerEndpoint());
    }
}
