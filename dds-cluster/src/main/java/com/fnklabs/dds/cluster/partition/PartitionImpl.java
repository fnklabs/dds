package com.fnklabs.dds.cluster.partition;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;

import java.io.Serializable;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Cluster partition data information contains Token range and state of Current partition
 */
public class Partition implements Serializable {
    /**
     * Start token range
     */
    private final PartitionKey start;

    /**
     * End token range
     */
    private final PartitionKey end;

    /**
     * Partition state
     */
    private final AtomicReference<PartitionState> state = new AtomicReference<>(PartitionState.OK);


    /**
     * @param start Token value
     * @param end   Token value
     * @param state Partition state
     */
    public Partition(PartitionKey start, PartitionKey end, PartitionState state) {
        this.start = start;
        this.end = end;
        this.state.set(state);
    }

    public PartitionState getState() {
        return state.get();
    }

    public PartitionKey getStart() {
        return start;
    }

    public PartitionKey getEnd() {
        return end;
    }

    public boolean changeState(PartitionState expectingState, PartitionState newState) {
        return state.compareAndSet(expectingState, newState);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(start, end);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Partition) {
            return Objects.equal(((Partition) obj).getStart(), getStart()) && Objects.equal(((Partition) obj).getEnd(), getEnd());
        }

        return false;
    }

    public boolean contains(PartitionKey key) {
        return getStart().compareTo(key) <= 0 && getEnd().compareTo(key) >= 0;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("Start", getStart()).add("End", getEnd()).toString();
    }

}
