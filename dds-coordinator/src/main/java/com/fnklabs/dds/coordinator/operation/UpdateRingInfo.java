package com.fnklabs.dds.coordinator.operation;

import com.fnklabs.dds.coordinator.ClusterInformation;

public class UpdateRingInfo implements Operation {
    private final ClusterInformation clusterInformation;

    public UpdateRingInfo(ClusterInformation clusterInformation) {
        this.clusterInformation = clusterInformation;
    }

    public ClusterInformation getClusterInformation() {
        return clusterInformation;
    }
}
