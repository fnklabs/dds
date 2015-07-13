package com.fnklabs.dds.coordinator;

public enum NodeStatus {
    /**
     * Node is up and available for any operation
     */
    UP,
    /**
     * Node is down and doesn't available for any operation
     */
    DOWN,
    /**
     * Node is up but balancing operation in progress (uploading) and it's available only for read/write operations
     */
    SEED,
    /**
     * Node is up but balancing operation in progress (downloading) and it's available only for write operations
     */
    PEER,

    /**
     * Node is up (just started) but synchronization in progress and it's doesn't available for any operations
     */
    SYNCHRONIZATION
}
