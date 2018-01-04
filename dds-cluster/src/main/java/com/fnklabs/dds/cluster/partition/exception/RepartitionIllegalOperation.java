package com.fnklabs.dds.coordinator.partition.exception;

public class RepartitionIllegalOperation extends RuntimeException {
    private final int replicationFactor;
    private final int members;

    public RepartitionIllegalOperation(int replicationFactor, int members) {
        super(String.format("Invalid operation. Requested replication factor: %d current members count: %d", replicationFactor, members));

        this.replicationFactor = replicationFactor;
        this.members = members;

    }
}
