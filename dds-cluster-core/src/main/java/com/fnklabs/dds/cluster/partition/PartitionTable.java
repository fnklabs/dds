package com.fnklabs.dds.cluster;

public interface PartitionTable {
    Node getOwner(Partition partition);
}
