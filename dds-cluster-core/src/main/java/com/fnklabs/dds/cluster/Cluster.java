package com.fnklabs.dds.cluster;

public interface Cluster {
    Result execute(Operation operation, ConsistencyLevel consistency);
}
