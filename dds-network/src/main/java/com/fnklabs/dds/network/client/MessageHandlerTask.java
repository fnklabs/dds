package com.fnklabs.dds.network.client;

import com.codahale.metrics.Timer;
import com.fnklabs.dds.Metrics;
import com.fnklabs.dds.network.Message;
import com.fnklabs.dds.network.ResponseFuture;
import org.jetbrains.annotations.NotNull;

import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Handler for processing system messages from server (notifications) or messages that wasn't awaiting
 */
class MessageHandlerTask implements Runnable {
    @NotNull
    private final ExecutorService executorService;
    @NotNull
    private final ConcurrentLinkedQueue<Message> inputMessages;
    @NotNull
    private final Consumer<Message> messageConsumer;

    @NotNull
    private final Map<Long, ResponseFuture> responseFutureMap;

    @NotNull
    private final AtomicBoolean isRunning;

    public MessageHandlerTask(@NotNull ExecutorService executorService,
                              @NotNull ConcurrentLinkedQueue<Message> inputMessages,
                              @NotNull Consumer<Message> messageConsumer,
                              @NotNull Map<Long, ResponseFuture> responseFutureMap,
                              @NotNull AtomicBoolean isRunning) {
        this.executorService = executorService;
        this.inputMessages = inputMessages;
        this.messageConsumer = messageConsumer;
        this.responseFutureMap = responseFutureMap;
        this.isRunning = isRunning;
    }

    @Override
    public void run() {
        if (isRunning.get()) {

            Message message = inputMessages.poll();

            if (message != null) {
                Timer.Context time = Metrics.getTimer(Metrics.Type.NET_CLIENT_PROCESSED_EVENTS).time();

                if (!responseFutureMap.containsKey(message.getReplyMessageId())) {
                    messageConsumer.accept(message);
                } else {
                    ResponseFuture responseFuture = responseFutureMap.get(message.getReplyMessageId());

                    responseFuture.onResponse(message);
                }

                time.stop();
            }

            executorService.submit(this);
        }
    }
}
