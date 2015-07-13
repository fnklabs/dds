package com.fnklabs.dds.network;

public enum OperationType implements Operation {

    /**
     * Map/Reduce operations
     */
    MAP(1),
    REDUCE(2),
    /**
     * Data transformation operations
     */
    GROUP(3),
    JOIN(4),

    /**
     * IO operations
     */
    DOWNLOAD(100),

    /**
     * DDS operations
     */
    CREATE_DDS(200),
    DELETE_DDS(201),


    /**
     * Notify current node that specified node was up and register in the ring
     */
    NODE_UP(1000),


    /**
     * Update node information
     */
    NODE_UPDATE_INFO(1005),

    /**
     * Notify current node that specified node was down and remove from the ring
     * If node is shutting down in normal case it can send request to coordinator about it
     */
    NODE_DOWN(1010),

    /**
     * Retrieve information about ring and do not register in the ring
     */
    CLUSTER_INFO(1020),

    /**
     * Send ping message (PING)
     */
    PING(1021),

    /**
     * Retrieve information about ring and do not register in the ring
     */
    UPDATE_CLUSTER_INFO(1030),

    /**
     * Retrieve notification when ring information was updated
     */
    UPDATE_RING_INFO(1031),


    /**
     * Elect new coordinator
     */
    ELECT_COORDINATOR(1100),

    /**
     * Notify about new coordinator
     */
    ELECTED_COORDINATOR(1110),


    //
    UNKNOWN(-100),;
    /**
     * Int value
     */
    private int value;

    OperationType(int value) {
        this.value = value;
    }

    public static OperationType valueOf(int value) {
        for (OperationType operationType : values()) {
            if (operationType.getValue() == value) {
                return operationType;
            }
        }

        throw new IllegalArgumentException(String.format("Invalid operation value: %d", value));
    }


    public int getValue() {
        return value;
    }

    @Override
    public int getCode() {
        return getValue();
    }
}
