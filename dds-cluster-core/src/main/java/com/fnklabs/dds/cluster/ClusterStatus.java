package com.fnklabs.dds.cluster;

/**
 * Cluster status
 */
enum ClusterStatus {

    /**
     * Cluster running in normal mode
     */
    OK,

    /**
     * If repartition needed when new node was up or one of the members node is down
     */
    INCONSISTENT,

    /**
     * When cluster status is unknown or can't be retrieved
     */
    UNKNOWN,
}
