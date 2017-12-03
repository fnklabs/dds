package com.fnklabs.dds.network;

import java.nio.ByteBuffer;

/**
 * Base message structure
 * <pre>
 *     | ID   | Reply ID | API_VERSION | SIZE    | DATA           |
 *     |------|----------|-------------|---------|----------------|
 *     | Long |  Long    | Integer     | Integer |  byte[]        |
 *     |------|----------|-------------|---------|----------------|
 *     | 0-7  | 8-15     | 16 - 19     | 20 - 23 |  23 + SIZE     |
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
     * Get id for reply message
     *
     * @return id for reply message or 0 if is not reply
     */
    long getReplyId();

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
