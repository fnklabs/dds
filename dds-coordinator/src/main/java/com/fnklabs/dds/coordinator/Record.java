package com.fnklabs.dds.coordinator;

import java.io.Serializable;

public interface Record extends Serializable {
    /**
     * Max record key size
     */
    int MAX_KEY_SIZE = 128;

    long serialVersionUID = 1L;

    /**
     * Get record key (128 bit)
     *
     * @return Record key
     */
    byte[] getKey();
}
