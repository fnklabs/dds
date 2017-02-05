package com.fnklabs.dds.coordinator;

import com.google.common.base.MoreObjects;
import com.google.common.base.Objects;
import com.google.common.net.HostAndPort;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.joda.time.DateTime;

import java.io.Serializable;
import java.util.UUID;

/**
 * Node information contains node address and version
 */
public final class NodeInfo implements Serializable, Comparable<NodeInfo> {

    private static final long serialVersionUID = 2046031529312431612L;

    /**
     * Node id
     */
    @NotNull
    private final UUID id;

    /**
     * Node address
     */
    @NotNull
    private final HostAndPort address;

    /**
     * Node version
     */
    private final int version;

    /**
     * Node status
     */
    @NotNull
    private final Node.NodeStatus status;

    /**
     * Last time when current node was updated
     */
    @NotNull
    private final DateTime lastUpdated = DateTime.now();

    /**
     * @param id      Node id
     * @param address Node address
     * @param version Node api version number
     * @param status  Node status
     */
    public NodeInfo(@NotNull UUID id, @NotNull HostAndPort address, int version, @NotNull Node.NodeStatus status) {
        this.id = id;
        this.address = address;
        this.version = version;
        this.status = status;
    }

    @NotNull
    public UUID getId() {
        return id;
    }

    @NotNull
    public Node.NodeStatus getStatus() {
        return status;
    }

    @NotNull
    public DateTime getLastUpdated() {
        return lastUpdated;
    }

    @NotNull
    public HostAndPort getAddress() {
        return address;
    }

    public int getVersion() {
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
        return MoreObjects
                .toStringHelper(this)
                .add("Host", getAddress().toString())
                .add("Version", getVersion())
                .add("Status", getStatus())
                .toString();
    }
}
