package com.fnklabs.dds.cluster;

public enum ConsistencyLevel {
    ONE((byte) 0, new ConsistencyOneExecutor()),
    QUORUM((byte) 1, new ConsistencyOneExecutor()),
    ALL((byte) 2, new ConsistencyOneExecutor());

    private final byte code;

    private final ConsistencyFunction consistencyFunction;

    ConsistencyLevel(byte code, ConsistencyFunction consistencyFunction) {
        this.code = code;
        this.consistencyFunction = consistencyFunction;
    }

    public ConsistencyFunction getConsistencyFunction() {
        return consistencyFunction;
    }

    public byte getCode() {
        return code;
    }
}
