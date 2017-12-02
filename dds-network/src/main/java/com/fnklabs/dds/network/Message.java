package com.fnklabs.dds.network;

import java.nio.ByteBuffer;

/**
 * Base message structure
 * <pre>
 *     | ID   | API_VERSION | SIZE    | DATA           |
 *     |------|-------------|---------|----------------|
 *     | Long | Integer     | Integer |  byte[]        |
 *     |------|-------------|---------|----------------|
 *     | 0-7  | 8-11        | 12 - 15 | 16 - 16 + SIZE |
 * </pre>
 */
public interface Message {
    public static final int HEADER_SIZE = 8 + 4 + 4;

    /**
     * Get message id
     *
     * @return Message id
     */
    long getId();

    /**
     * Api version
     *
     * @return Api version
     */
    ApiVersion getVersion();

    /**
     * Get message size
     *
     * @return
     */
    int getDataSize();

    /**
     * Get message size
     *
     * @return
     */
    int getSize();

    /**
     * Get message data
     *
     * @return data
     */
    byte[] getData();

    /**
     * Write message to buffer
     *
     * @param buffer Buffer
     */
    void read(ByteBuffer buffer);

    /**
     * Read message from buffer
     *
     * @param buffer buffer
     */
    void write(ByteBuffer buffer);
}
