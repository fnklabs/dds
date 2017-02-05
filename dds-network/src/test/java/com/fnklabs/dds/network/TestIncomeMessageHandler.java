package com.fnklabs.dds.network;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.nio.ByteBuffer;

@Service
@Slf4j
class TestIncomeMessageHandler implements IncomeMessageHandler {

    @Override
    public ListenableFuture<ByteBuffer> handle(ByteBuffer message) {
        return Futures.immediateFuture(message);
    }
}
