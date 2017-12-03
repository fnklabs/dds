package com.fnklabs.dds.network.server;

import com.fnklabs.dds.network.ApiVersion;
import com.fnklabs.dds.network.ReplyMessage;
import com.fnklabs.dds.network.RequestMessage;
import com.fnklabs.dds.network.StatusCode;
import com.fnklabs.metrics.MetricsFactory;
import com.fnklabs.metrics.Timer;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.jetbrains.annotations.Nullable;

import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

/**
 * Worker for processing new messages from Connector queue
 * Retrieve new messages from {@link #messagesQueue} queue and consume them to {@link #incomeMessageHandler} consumer
 */
@Slf4j
@RequiredArgsConstructor
class NetworkServerWorker implements Runnable {

    /**
     * New request queue
     */
    private final Queue<RequestMessage> messagesQueue;

    /**
     * Is running
     */
    private final AtomicBoolean isRunning;


    private final IncomeMessageHandler incomeMessageHandler;

    /**
     * Response consumer
     */
    private final BiConsumer<Long, ReplyMessage> responseConsumer;


    @Override
    public void run() {
        while (isRunning.get()) {
            RequestMessage message = messagesQueue.poll();

            if (message != null) {
                log.debug("Received new message: {}", message);

                onMessage(message);
            }
        }
    }

    /**
     * Process new messages from queue
     */
    private void onMessage(RequestMessage message) {
        Timer timer = MetricsFactory.getMetrics().getTimer("network.server.message.process");

        ListenableFuture<ByteBuffer> handle = incomeMessageHandler.handle(ByteBuffer.wrap(message.getData()));

        Futures.addCallback(handle, new FutureCallback<ByteBuffer>() {
            @Override
            public void onSuccess(@Nullable ByteBuffer result) {
                byte[] data = result != null ? result.array() : null;

                log.debug("Reply {} on {}", data, message);

                ReplyMessage msg = new ReplyMessage(ReplyMessage.ID.incrementAndGet(), message.getId(), ApiVersion.CURRENT, data);

                timer.stop();

                responseConsumer.accept(message.getSessionId(), msg);
            }

            @Override
            public void onFailure(Throwable t) {
                log.warn("Cant process message: {}", message, t);
                timer.stop();

                responseConsumer.accept(message.getSessionId(), new ReplyMessage(ReplyMessage.ID.incrementAndGet(), message.getId(), ApiVersion.CURRENT, StatusCode.OK, null));
            }
        });
    }
}
