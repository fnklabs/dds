package com.fnklabs.dds.coordinator.operation;

import com.fnklabs.dds.coordinator.NodeInfo;

public class NodeDown implements Operation {
    private final NodeInfo nodeInfo;

    public NodeDown(NodeInfo nodeInfo) {
        this.nodeInfo = nodeInfo;
    }

    public NodeInfo getNodeInfo() {
        return nodeInfo;
    }
}
