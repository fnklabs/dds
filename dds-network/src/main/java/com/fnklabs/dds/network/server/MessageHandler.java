package com.fnklabs.dds.network.server;

import com.fnklabs.dds.network.Message;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * Request message handler
 */
public interface MessageHandler {
    /**
     * Handler message
     *
     * @param message Message
     */
    ListenableFuture<Message> handle(Message message);
}
