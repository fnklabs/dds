package com.fnklabs.dds.network;

import com.google.common.util.concurrent.ListenableFuture;

import java.nio.ByteBuffer;

/**
 * New message handler interface that must be implemented to process new messages from clients that must be passed to {@link NetworkServerWorker}
 */
public interface IncomeMessageHandler {
    /**
     * Handle and process new message from server
     *
     * @param message Message data
     *
     * @return Future for response data
     */
    ListenableFuture<ByteBuffer> handle(ByteBuffer message);
}
