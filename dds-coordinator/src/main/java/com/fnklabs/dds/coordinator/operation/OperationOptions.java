package com.fnklabs.dds.coordinator.operation;

import com.fnklabs.dds.coordinator.ConsistencyLevel;
import com.fnklabs.dds.coordinator.LoadBalancingPolicy;

public class OperationOptions {

    public static final int DEFAULT_RETRY_COUNT = 1;
    /**
     * Default consistency level
     */
    private static final ConsistencyLevel DEFAULT_CONSISTENCY_LEVEL = ConsistencyLevel.QUORUM;
    /**
     * Consistency level
     */
    private ConsistencyLevel consistencyLevel = DEFAULT_CONSISTENCY_LEVEL;

    /**
     * Number of retry count before operation can completed with error
     */
    private int retryCount = DEFAULT_RETRY_COUNT;

    /**
     * Load balancing policy
     */
    private LoadBalancingPolicy loadBalancingPolicy;

    public OperationOptions(ConsistencyLevel consistencyLevel, LoadBalancingPolicy loadBalancingPolicy, int retryNumber) {
        this.consistencyLevel = consistencyLevel;
        this.loadBalancingPolicy = loadBalancingPolicy;
        this.retryCount = retryNumber;
    }

    public ConsistencyLevel getConsistencyLevel() {
        return consistencyLevel;
    }

    public LoadBalancingPolicy getLoadBalancingPolicy() {
        return loadBalancingPolicy;
    }

    public int getRetryCount() {
        return retryCount;
    }
}
