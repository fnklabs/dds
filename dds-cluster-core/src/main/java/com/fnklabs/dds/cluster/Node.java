package com.fnklabs.dds.cluster;

import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.ListenableFuture;
import org.jetbrains.annotations.NotNull;
import org.joda.time.DateTime;

import java.io.Closeable;
import java.util.UUID;

/**
 * Cluster node instance
 */
public interface Node extends Closeable, Comparable<Node> {
    /**
     * Default coordinator port
     */
    int DEFAULT_PORT = 10000;

    /**
     * Get node id
     *
     * @return
     */
    UUID getId();

    /**
     * Get node status
     *
     * @return
     */
    NodeStatus getStatus();

    /**
     * get node info last updated information
     *
     * @return
     */
    DateTime getLastUpdated();

    /**
     * Get node address
     *
     * @return
     */
    HostAndPort getAddress();

    /**
     * Get node cluster version
     *
     * @return
     */
    ClusterVersion getVersion();

    Configuration getConfiguration();

    /**
     * sort nodes by id
     *
     * @param o
     *
     * @return
     */
    @Override
    default int compareTo(@NotNull Node o) {
        return getId().compareTo(o.getId());
    }

    /**
     * Notify node that specified node was up and register it in the cluster
     * <p>
     * Set up new node into to the cluster if {@link Node#getClass()} is null
     *
     * @param Node New node info
     *
     * @return Future for getInstance NodeUp operation and return null on future error
     */
    ListenableFuture<Cluster> nodeUp(Node Node);

    /**
     * Notify node that specified node was down and remove it from the cluster
     *
     * @param Node Node info
     *
     * @return Future for NodeDown operation and return null on future error
     */
    ListenableFuture<Boolean> nodeDown(Node Node);

    /**
     * Run repair operation in cluster.
     * <p>
     * Must recalculate Partition table and start repartition operation
     *
     * @param clusterInformation New cluster information by which repartition must be done
     *
     * @return Future for repair operation
     */
    ListenableFuture<Boolean> repair(Cluster clusterInformation);

    /**
     * Get current node information
     *
     * @return Node information
     */
    Node getNode();

    /**
     * Retrieve information about cluster from current node
     *
     * @return Future for getInstance RingInfo operation and return null on future error
     */
    ListenableFuture<Boolean> update();

    /**
     * Notification about ring info was updated
     *
     * @param clusterInformation New ring information
     *
     * @return true if RingInfo was successfully update false otherwise
     */
    ListenableFuture<Boolean> updateClusterInfo(Cluster clusterInformation);

    /**
     * Ping host and return latency (diff of time when ping messages was send to remote node and when reply was retrieved)
     *
     * @return Future for Ping operation and return null on future error
     */
    ListenableFuture<Long> ping();


}
