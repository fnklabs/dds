package com.fnklabs.dds.coordinator.operation;

import com.fnklabs.dds.coordinator.NodeInfo;

import java.io.Serializable;
import java.util.SortedSet;

/**
 * Elect new coordinator operation
 * <p>
 * All members from ring must add theirs address to elected set
 */
public class Elect extends DistributedOperation implements Serializable {

    /**
     * Elected nodes (nodes that was registered in chain)
     */
    private final SortedSet<NodeInfo> elected;

    private final long created = System.currentTimeMillis();

    public Elect(SortedSet<NodeInfo> elected) {
        this.elected = elected;
    }

    public long getCreated() {
        return created;
    }

    public SortedSet<NodeInfo> getElected() {
        return elected;
    }
}
