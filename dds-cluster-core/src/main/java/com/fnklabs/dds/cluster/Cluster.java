package com.fnklabs.dds.cluster;

import com.fnklabs.dds.cluster.partition.PartitionTable;

import java.util.SortedSet;
import java.util.function.Function;

/**
 * Facade to work with cluster
 */
public interface Cluster {
    /**
     * Execute operation in cluster with provided consistencyLevel
     *
     * @param operation        Operation to execute
     * @param consistencyLevel Consistency level by which operation must be executed
     *
     * @return Result
     */
    <T extends Result> T execute(Function<Node, T> operation, ConsistencyLevel consistencyLevel);

    Cluster getClusterInformation(ConsistencyLevel consistencyLevel);

    PartitionTable getPartitionTable();

    /**
     * Get cluster version (updated on each topology change)
     *
     * @return cluster version
     */
    long version();

    /**
     * Return cluster members sorted by {@link Node#getId()}
     *
     * @return cluster members
     */
    SortedSet<Node> getMembers();

    /**
     * get cluster status
     *
     * @return
     */
    ClusterStatus getClusterStatus();


    /**
     * Cluster sender (who send cluster information)
     *
     * @return
     */
    Node getSender();
}
