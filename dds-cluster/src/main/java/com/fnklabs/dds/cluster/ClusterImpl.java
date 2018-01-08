package com.fnklabs.dds.cluster;

import com.fnklabs.dds.cluster.partition.PartitionTable;
import com.google.common.net.HostAndPort;

import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

public class ClusterImpl implements Cluster {

    private final Set<HostAndPort> seeds;

    private transient final ClientNodeRegistry clientNodeRegistry;

    private transient final Serializer serializer;

    private final SortedSet<Node> members = new TreeSet<>();

    private final AtomicLong version = new AtomicLong();

    private final AtomicReference<ClusterStatus> status = new AtomicReference<>(ClusterStatus.UNKNOWN);

    public ClusterImpl(Set<HostAndPort> seeds, ClientNodeRegistry clientNodeRegistry, Serializer serializer) {
        this.seeds = seeds;
        this.clientNodeRegistry = clientNodeRegistry;
        this.serializer = serializer;
    }

    @Override
    public <T extends Result> T execute(Function<Node, T> function, ConsistencyLevel consistencyLevel) {
        return consistencyLevel.getConsistencyFunction()
                               .execute(members, function);
    }

    @Override
    public Cluster getClusterInformation(ConsistencyLevel consistencyLevel) {
        return null;
    }

    @Override
    public PartitionTable getPartitionTable() {
        return null;
    }

    @Override
    public long version() {
        return version.get();
    }

    @Override
    public SortedSet<Node> getMembers() {
        return null;
    }

    @Override
    public ClusterStatus getClusterStatus() {
        return null;
    }

    @Override
    public Node getSender() {
        return null;
    }
}
