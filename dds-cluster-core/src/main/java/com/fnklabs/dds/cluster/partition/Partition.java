package com.fnklabs.dds.cluster.partition;

import com.google.common.collect.Range;
import org.jetbrains.annotations.NotNull;

public interface Partition extends Comparable<Partition> {
    Range<Long> range();

    PartitionState state();

    @Override
    default int compareTo(@NotNull Partition o) {
        return Long.compare(range().lowerEndpoint(), o.range().lowerEndpoint());
    }

    boolean owned(long token);
}
