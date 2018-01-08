package com.fnklabs.dds.cluster;

/**
 * Node status normal flow:
 * <pre>
 *
 * START_UP -> {DIRTY, SYNCHRONIZE} -> {UP, DOWN}
 *
 * </pre>
 */
public enum NodeStatus {
    /**
     * Indicate when node was just start up
     */
    START_UP,

    /**
     * Node was start up but doesn't join cluster
     */
    DIRTY,

    /**
     * Synchronization data in cluster (remove down down) and run repartition over cluster
     */
    SYNCHRONIZE,

    /**
     * Node is up and available for any operation
     */
    UP,

    /**
     * Node is down and doesn't available for any operation
     */
    DOWN,


}
