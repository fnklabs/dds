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
    private NodeInfo sender;

    private SortedSet<NodeInfo> elected;

    private long created = System.currentTimeMillis();

    public Elect(NodeInfo sender, SortedSet<NodeInfo> elected) {
        this.sender = sender;
        this.elected = elected;
    }

    public long getCreated() {
        return created;
    }

    public NodeInfo getSender() {
        return sender;
    }

    public SortedSet<NodeInfo> getElected() {
        return elected;
    }
}
