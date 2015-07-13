package com.fnklabs.dds.coordinator;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.net.HostAndPort;
import org.jetbrains.annotations.Nullable;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

/**
 * Node information contains node address and version
 */
public class NodeInfo implements Serializable, Comparable<NodeInfo> {

    /**
     * Node address
     */
    private final HostAndPort address;

    /**
     * Node version
     */
    private final String version;

    /**
     * Buckets set that current node is owning
     */
    private final Set<Bucket> buckets = new HashSet<>();
    /**
     * Mirrors set for current node
     */
    private final Set<NodeInfo> mirrors = new HashSet<>();
    /**
     * Node status
     */
    private NodeStatus status = NodeStatus.SYNCHRONIZATION;
    /**
     * Last time when current node was updated
     */
    private long lastUpdated = System.currentTimeMillis();

    public NodeInfo(HostAndPort address, String version) {
        this.address = address;
        this.version = version;
    }

    public Set<Bucket> getBuckets() {
        return buckets;
    }

    public Set<NodeInfo> getMirrors() {
        return mirrors;
    }

    public NodeStatus getStatus() {
        return status;
    }

    public long getLastUpdated() {
        return lastUpdated;
    }

    public HostAndPort getAddress() {
        return address;
    }

    public String getVersion() {
        return version;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof NodeInfo)) {
            return false;
        }

        return ((NodeInfo) obj).getAddress().equals(getAddress());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(getAddress());
    }

    /**
     * Compare nodes by address
     *
     * @param nodeInfo NodeInfo
     *
     * @return reus
     */
    @Override
    public int compareTo(@Nullable NodeInfo nodeInfo) {
        if (nodeInfo == null) {
            throw new NullPointerException();
        }

        return getAddress().toString().compareTo(nodeInfo.getAddress().toString());
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("Host", getAddress().toString()).add("Version", getVersion()).toString();
    }
}
