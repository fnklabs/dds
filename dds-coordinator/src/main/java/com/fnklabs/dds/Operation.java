package com.fnklabs.dds;

/**
 * Operation interface
 */
public interface Operation {

    /**
     * Get operation type code. Operation code is used for operation deserialization
     *
     * @return Operation type code
     */
    int getCode();

    byte[] getData();


}