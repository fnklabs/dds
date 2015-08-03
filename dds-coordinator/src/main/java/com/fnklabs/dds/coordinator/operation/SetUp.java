package com.fnklabs.dds.coordinator.operation;

import com.fnklabs.dds.coordinator.NodeInfo;

/**
 * SetUp new node in the cluster operation.
 * Must be called by new (clean) node when node joining cluster
 */
public class SetUp implements Operation {
    private final NodeInfo nodeInfo;

    public SetUp(NodeInfo nodeInfo) {
        this.nodeInfo = nodeInfo;
    }

    public NodeInfo getNodeInfo() {
        return nodeInfo;
    }
}
