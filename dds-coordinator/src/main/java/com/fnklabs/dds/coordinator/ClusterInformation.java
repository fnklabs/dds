package com.fnklabs.dds.coordinator;

import com.fnklabs.dds.coordinator.partition.PartitionTable;
import com.google.common.base.MoreObjects;
import org.jetbrains.annotations.NotNull;
import org.joda.time.DateTime;

import java.io.Serializable;
import java.util.Collections;
import java.util.SortedSet;

/**
 * Cluster information. Immutable object
 * <p>
 * Contains data about cluster:
 * - who is coordinator
 * - cluster members
 */
public class ClusterInformation implements Serializable, Comparable<ClusterInformation> {

    private static final long serialVersionUID = -5608399972816888801L;
    /**
     * Current cluster coordinator
     */
    @NotNull
    private final NodeInfo coordinator;

    /**
     * Current cluster active members
     */
    @NotNull
    private final SortedSet<NodeInfo> members;

    /**
     * Information about node from which data was retrieved
     */
    @NotNull
    private final NodeInfo sender;

    /**
     * Current cluster status
     */
    @NotNull
    private final ClusterStatus clusterStatus;

    /**
     * Cluster partition table
     */
    @NotNull
    private final PartitionTable partitionTable;

    /**
     * DateTime of data
     */
    @NotNull
    private final DateTime created = DateTime.now();

    /**
     * @param coordinator    Current cluster coordinator
     * @param members        Cluster active members
     * @param sender         Node Info about node that own current data
     * @param clusterStatus  Current cluster status
     * @param partitionTable Cluster partition table
     */
    public ClusterInformation(@NotNull NodeInfo coordinator,
                              @NotNull SortedSet<NodeInfo> members,
                              @NotNull NodeInfo sender,
                              @NotNull ClusterStatus clusterStatus,
                              @NotNull PartitionTable partitionTable) {
        this.coordinator = coordinator;
        this.members = members;
        this.sender = sender;
        this.clusterStatus = clusterStatus;
        this.partitionTable = partitionTable;
    }

    @NotNull
    public PartitionTable getPartitionTable() {
        return partitionTable;
    }

    @NotNull
    public DateTime getCreated() {
        return created;
    }

    @NotNull
    public NodeInfo getCoordinator() {
        return coordinator;
    }

    @NotNull
    public SortedSet<NodeInfo> getMembers() {
        return Collections.unmodifiableSortedSet(members);
    }

    @NotNull
    public ClusterStatus getClusterStatus() {
        return clusterStatus;
    }

    @NotNull
    public NodeInfo getSender() {
        return sender;
    }

    @Override
    public int compareTo(@NotNull ClusterInformation o) {
        return o.getCreated().compareTo(getCreated());
    }

    @Override
    public String toString() {
        return MoreObjects
                .toStringHelper(this)
                .add("coordinator", getCoordinator())
                .add("members", getMembers())
                .add("cluster status", getClusterStatus())
                .add("created", getCreated())
                .toString();
    }
}
