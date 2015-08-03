package com.fnklabs.dds.coordinator.operation;

import com.fnklabs.dds.coordinator.NodeInfo;

public class Elected extends DistributedOperation {
    private NodeInfo nodeInfo;

    public Elected(NodeInfo nodeInfo) {
        this.nodeInfo = nodeInfo;
    }

    public NodeInfo getNodeInfo() {
        return nodeInfo;
    }
}
