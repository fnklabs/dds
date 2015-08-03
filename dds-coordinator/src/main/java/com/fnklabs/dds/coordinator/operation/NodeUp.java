package com.fnklabs.dds.coordinator.operation;

import com.fnklabs.dds.coordinator.NodeInfo;

public class NodeUp implements Operation {
    private final NodeInfo nodeInfo;

    public NodeUp(NodeInfo nodeInfo) {
        this.nodeInfo = nodeInfo;
    }

    public NodeInfo getNodeInfo() {
        return nodeInfo;
    }
}
