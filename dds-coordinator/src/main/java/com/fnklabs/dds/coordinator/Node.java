package com.fnklabs.dds.coordinator;

import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.ListenableFuture;

import java.io.Closeable;

/**
 * Cluster node instance
 */
public interface Node extends Closeable {
    /**
     * Default coordinator port
     */
    int DEFAULT_PORT = 10000;

    /**
     * Get node host address
     *
     * @return Host address
     */
    HostAndPort getAddressAndPort();

    /**
     * Notify node that specified node was up and register it in the cluster
     * <p>
     * Set up new node into to the cluster if {@link NodeInfo#getClass()} is null
     *
     * @param nodeInfo New node info
     *
     * @return Future for getInstance NodeUp operation and return null on future error
     */
    ListenableFuture<ClusterInformation> nodeUp(NodeInfo nodeInfo);

    /**
     * Notify node that specified node was down and remove it from the cluster
     *
     * @param nodeInfo Node info
     *
     * @return Future for NodeDown operation and return null on future error
     */
    ListenableFuture<Boolean> nodeDown(NodeInfo nodeInfo);

    /**
     * Run repair operation in cluster.
     * <p>
     * Must recalculate Partition table and start repartition operation
     *
     * @param clusterInformation New cluster information by which repartition must be done
     *
     * @return Future for repair operation
     */
    ListenableFuture<Boolean> repair(ClusterInformation clusterInformation);

    /**
     * Get current node information
     *
     * @return Node information
     */
    ListenableFuture<NodeInfo> getNodeInfo();

    /**
     * Retrieve information about cluster from current node
     *
     * @return Future for getInstance RingInfo operation and return null on future error
     */
    ListenableFuture<ClusterInformation> getClusterInfo();

    /**
     * Notification about ring info was updated
     *
     * @param clusterInformation New ring information
     *
     * @return true if RingInfo was successfully update false otherwise
     */
    ListenableFuture<Boolean> updateClusterInfo(ClusterInformation clusterInformation);

    /**
     * Ping host and return latency (diff of time when ping messages was send to remote node and when reply was retrieved)
     *
     * @return Future for Ping operation and return null on future error
     */
    ListenableFuture<Long> ping();

    /**
     * Node status normal flow:
     * <pre>
     *
     *
     * START_UP -> STARTING_UP -> SETUP -> SETTING_UP -> REPAIR -> SYNCHRONIZATION -> UP
     *
     * </pre>
     */
    enum NodeStatus {
        /**
         * If node status is unknown. Indicate when node was just start up
         */
        START_UP,

        STARTING_UP,

        /**
         * Setup new node into the cluster and run repartition over cluster
         */
        SETUP,

        /**
         * Setting up node in the cluster
         */
        SETTING_UP,


        /**
         * Repair cluster (remove down down) and run repartition over cluster
         */
        REPAIR,

        /**
         * Node is up (just started) but synchronization in progress and it's doesn't available for any operations
         */
        SYNCHRONIZATION,

        /**
         * Node is up and available for any operation
         */
        UP,

        /**
         * Node is down and doesn't available for any operation
         */
        DOWN,

        /**
         * If node is not responding to much on any operation
         */
        NOT_RESPOND,

        SHUTDOWN,
    }
}
