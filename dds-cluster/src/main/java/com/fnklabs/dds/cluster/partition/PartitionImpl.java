package com.fnklabs.dds.cluster.partition;

import com.google.common.collect.Range;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Cluster partition data information contains Token range and state of Current partition
 */
class PartitionImpl implements Partition {
    /**
     * token range
     */
    private final Range<Long> range;

    /**
     * Partition state
     */
    private final AtomicReference<PartitionState> state = new AtomicReference<>(PartitionState.OK);

    /**
     * @param start Token value
     * @param end   Token value
     * @param state Partition state
     */
    PartitionImpl(long start, long end, PartitionState state) {
        this.range = Range.closedOpen(start, end);
        this.state.set(state);
    }

    @Override
    public Range<Long> range() {
        return range;
    }

    @Override
    public PartitionState state() {
        return state.get();
    }

    @Override
    public boolean owned(long token) {
        return range.contains(token);
    }
}
