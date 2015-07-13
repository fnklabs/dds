package com.fnklabs.dds.coordinator;

import java.io.Serializable;

public enum ConsistencyLevel implements Serializable {
    ALL,
    QUORUM,
    ONE,
}
