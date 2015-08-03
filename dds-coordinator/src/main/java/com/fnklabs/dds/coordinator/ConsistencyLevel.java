package com.fnklabs.dds.coordinator;

public enum ConsistencyLevel {
    /**
     * All members of cluster must respond on operation
     */
    ALL,
    /**
     * Total members \ Replication factor + 1 members must respond on operation
     */
    QUORUM,
    /**
     * Any number of members must > 0 must response on operation
     */
    ONE,
}
