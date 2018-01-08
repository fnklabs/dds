package com.fnklabs.dds.cluster;

import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.ListenableFuture;
import org.joda.time.DateTime;

import java.io.IOException;
import java.util.UUID;

/**
 * Rmi to node
 */
class NodeProxy implements Node {
    private final HostAndPort remoteAddress;

    private UUID id;

    private final Serializer serializer;

    NodeProxy(HostAndPort remoteAddress, Serializer serializer) {
        this.serializer = serializer;
        this.id = id;
        this.remoteAddress = remoteAddress;
    }

    @Override
    public UUID getId() {
        return id;
    }

    @Override
    public NodeStatus getStatus() {
        return null;
    }

    @Override
    public DateTime getLastUpdated() {
        return null;
    }

    @Override
    public HostAndPort getAddress() {
        return null;
    }

    @Override
    public Version getVersion() {
        return null;
    }

    @Override
    public Configuration getConfiguration() {
        return null;
    }


    @Override
    public ListenableFuture<Cluster> nodeUp(Node Node) {
        return null;
    }

    @Override
    public ListenableFuture<Boolean> nodeDown(Node Node) {
        return null;
    }

    @Override
    public ListenableFuture<Boolean> repair(Cluster clusterInformation) {
        return null;
    }

    @Override
    public Node getNode() {
        return null;
    }

    @Override
    public ListenableFuture<Boolean> update() {
        return null;
    }

    @Override
    public ListenableFuture<Boolean> updateClusterInfo(Cluster clusterInformation) {
        return null;
    }

    @Override
    public ListenableFuture<Long> ping() {
        return null;
    }

    @Override
    public void close() throws IOException {

    }
}
