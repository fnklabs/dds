package com.fnklabs.dds.cluster;

public enum ConsistencyLevel {
    ONE((byte) 0),
    QUORUM((byte) 1),
    ALL((byte) 2);

    private final byte code;

    ConsistencyLevel(byte code) {this.code = code;}

    public byte getCode() {
        return code;
    }
}
