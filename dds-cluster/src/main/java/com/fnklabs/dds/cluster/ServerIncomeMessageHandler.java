package com.fnklabs.dds.cluster;

import com.fnklabs.dds.network.server.IncomeMessageHandler;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;

class ServerIncomeMessageHandler implements IncomeMessageHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(ServerIncomeMessageHandler.class);
    /**
     * NodeImpl instance
     */
    private final ServerNodeImpl server;


    /**
     * @param server NodeImpl instance
     */
    ServerIncomeMessageHandler(ServerNodeImpl server) {
        this.server = server;
    }

    @Override
    public ListenableFuture<ByteBuffer> handle(ByteBuffer message) {
        return null;
    }
}
