package com.fnklabs.dds.coordinator;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * RoundRobbin balance policy will select next node from Circle (moving from left to right)
 */
public class RoundRobinLoadBalancingPolicy implements LoadBalancingPolicy {
    private List<NodeInfo> peers = Collections.synchronizedList(new ArrayList<>());

    private AtomicInteger peer = new AtomicInteger(0);

    public RoundRobinLoadBalancingPolicy() {
    }

    public RoundRobinLoadBalancingPolicy(List<NodeInfo> peers) {
        this.peers = peers;
    }

    @Override
    public void add(NodeInfo peer) {
        peers.add(peer);
    }

    @Override
    public void remove(NodeInfo peer) {
        peers.remove(peer);
    }

    @Override
    public NodeInfo next() {
        int nextPeer = peer.get();

        while (nextPeer > peers.size() - 1) {
            if (peer.compareAndSet(nextPeer, 0)) {
            }

            nextPeer = peer.get();
        }

        NodeInfo dataNode = peers.get(nextPeer);

        peer.getAndIncrement();

        return dataNode;
    }

    @Override
    public List<NodeInfo> getExecutionPlan() {
        ArrayList<NodeInfo> peersSnapshot = new ArrayList<>(peers);

        Collections.sort(peersSnapshot);

        return peersSnapshot;
    }


    @Override
    public int getPeersCount() {
        return peers.size();
    }
}
