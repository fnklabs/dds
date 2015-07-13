package com.fnklabs.dds.coordinator;

/**
 * Current node or ring status
 */
public enum RingStatus {
    /**
     * If current node was just started and need boot
     */
    STARTING,

    /**
     * If current node registering and retrieve information about node
     */
    BOOTING,

    /**
     * Ring status is ok
     */
    RUNNING,

    /**
     * Electing coordinator in ring is needed
     */
    ELECT,

    /**
     * Electing coordinator in ring in progress
     */
    ELECTING,

    /**
     * Synchronization ring (moving data or something else)
     */
    SYNCHRONIZATION,

    /**
     * If current node need to shutdown
     */
    SHUTDOWN,
}
