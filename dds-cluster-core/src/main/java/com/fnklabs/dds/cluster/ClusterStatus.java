package com.fnklabs.dds.cluster;

/**
 * Cluster status
 */
enum ClusterStatus {

    /**
     * Cluster running in normal mode
     */
    OK((byte) 0),

    /**
     * If repartition needed when new node was up or one of the members node is down
     */
    REPAIR((byte) 1),

    /**
     * When cluster status is unknown or can't be retrieved
     */
    UNKNOWN((byte) -1),;

    private final byte value;

    ClusterStatus(byte value) {this.value = value;}

    public byte getValue() {
        return value;
    }
}
