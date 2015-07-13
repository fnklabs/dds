package com.fnklabs.dds.network.connector;

import com.fnklabs.dds.network.StatusCode;

/**
 * Base message structure
 *
 * @param <T>
 */
public interface Message<T> {

    /**
     * Message id
     *
     * @return Return unique message id must be unique per session
     */
    long getId();

    /**
     * Message id
     *
     * @return Return unique message id must be unique per session
     */
    long getReplyMessageId();

    /**
     * Get client identification
     *
     * @return Client id
     */
    long getClientId();

    StatusCode getStatusCode();

    /**
     * Get operation code
     *
     * @return Operation Code
     */
    int getOperationCode();

    /**
     * Get message data
     *
     * @return Message data
     */
    T getData();
}
