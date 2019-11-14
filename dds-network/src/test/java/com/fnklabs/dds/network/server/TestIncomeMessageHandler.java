package com.fnklabs.dds.network.server;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import java.nio.ByteBuffer;


class TestIncomeMessageHandler implements IncomeMessageHandler {

    @Override
    public ListenableFuture<ByteBuffer> handle(ByteBuffer message) {
        return Futures.immediateFuture(message);
    }
}
