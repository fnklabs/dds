package com.fnklabs.dds.coordinator;

import java.util.List;

public interface LoadBalancingPolicy {
    /**
     * Add new peer to balance peers
     *
     * @param peer peer
     */
    void add(NodeInfo peer);

    /**
     * Remove peer from balance peers
     *
     * @param peer peer
     */
    void remove(NodeInfo peer);

    /**
     * Get next peer
     *
     * @return next peer
     */
    NodeInfo next();

    /**
     * Retrieve query plan for specified operation
     *
     * @return List of T for execution
     */
    List<NodeInfo> getExecutionPlan();

    /**
     * Get number of peers
     *
     * @return number of peers
     */
    int getPeersCount();
}
