package com.fnklabs.dds.coordinator;

import java.io.Serializable;
import java.util.SortedSet;

/**
 * Ring information
 */
public class RingInfo implements Serializable {

    /**
     * Current ring coordinator
     */
    private NodeInfo coordinator;

    /**
     * Current ring members
     */
    private SortedSet<NodeInfo> members;


    /**
     * timestamp of ring info creature
     */
    private long created = System.currentTimeMillis();

    /**
     * Information about who send current info
     */
    private NodeInfo sender;

    public RingInfo(NodeInfo coordinator, SortedSet<NodeInfo> members, NodeInfo sender) {
        this.coordinator = coordinator;
        this.members = members;
        this.sender = sender;
    }

    public RingInfo(NodeInfo coordinator, SortedSet<NodeInfo> members, long created, NodeInfo sender) {
        this.coordinator = coordinator;
        this.members = members;
        this.created = created;
        this.sender = sender;
    }


    public NodeInfo getCoordinator() {
        return coordinator;
    }

    public SortedSet<NodeInfo> getMembers() {
        return members;
    }

    public long getCreated() {
        return created;
    }

    public NodeInfo getSender() {
        return sender;
    }
}
