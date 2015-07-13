package com.fnklabs.dds.coordinator;

import com.google.common.net.HostAndPort;

public interface NodeFactory {
    Node get(HostAndPort hostAndPort);

    /**
     * Get node
     *
     * @param nodeInfo NodeInfo
     *
     * @return NodeInfo
     */
    default Node get(NodeInfo nodeInfo) {
        return get(nodeInfo.getAddress());
    }

    void remove(HostAndPort nodeInfo);
}
