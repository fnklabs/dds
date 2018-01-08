package com.fnklabs.dds.cluster;

import java.util.List;

public interface LoadBalancingPolicy {
    /**
     * Add new peer to balance peers
     *
     * @param peer peer
     */
    void add(Node peer);

    /**
     * Remove peer from balance peers
     *
     * @param peer peer
     */
    void remove(Node peer);

    /**
     * Get next peer
     *
     * @return next peer
     */
    Node next();

    /**
     * Retrieve query plan for specified operation
     *
     * @return List of T for execution
     */
    List<Node> getExecutionPlan();

    /**
     * Get number of peers
     *
     * @return number of peers
     */
    int getPeersCount();
}
