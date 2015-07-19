package com.fnklabs.dds.network.server;

import com.fnklabs.dds.network.Message;

/**
 * Request message handler
 */
public interface MessageHandler {
    /**
     * Handler message
     *
     * @param message Message
     */
    Message handle(Message message);
}
