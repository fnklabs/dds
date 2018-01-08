package com.fnklabs.dds.cluster.partition;

public enum PartitionState {
    /**
     * When partition in normal state
     */
    OK,
    /**
     * When repartition operation started on current partition
     */
    BALANCING,
    /**
     * When current partition was removed or replaced with new partition information
     */
    REMOVE
}
