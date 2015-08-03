package com.fnklabs.dds.network.server;

import com.codahale.metrics.Timer;
import com.fnklabs.dds.Metrics;
import com.fnklabs.dds.network.ApiVersion;
import com.fnklabs.dds.network.Message;
import com.fnklabs.dds.network.StatusCode;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.jetbrains.annotations.NotNull;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Retrieve new messages from {@link #newMessages} queue and consume them to {@link #messageHandler} consumer
 */
class ServerWorker implements Runnable {

    /**
     * New request queue
     */
    @NotNull
    private final ConcurrentLinkedQueue<Message> newMessages;

    /**
     * Is running
     */
    @NotNull
    private final AtomicBoolean isRun;

    /**
     * Response consumer
     */
    @NotNull
    private final MessageHandler messageHandler;

    /**
     * Executor service
     */
    @NotNull
    private final ExecutorService executorService;

    @NotNull
    private final Consumer<Message> responseConsumer;

    public ServerWorker(@NotNull ConcurrentLinkedQueue<Message> newMessages,
                        @NotNull AtomicBoolean isRunning,
                        @NotNull MessageHandler messageHandler,
                        @NotNull ExecutorService executorService,
                        @NotNull Consumer<Message> responseConsumer) {
        this.newMessages = newMessages;
        this.isRun = isRunning;
        this.executorService = executorService;
        this.messageHandler = messageHandler;
        this.responseConsumer = responseConsumer;
    }

    @Override
    public void run() {
        if (isRun.get()) {
            process();

            executorService.submit(this);
        }
    }

    /**
     *
     */
    private void process() {
        Message message = getNewMessages().poll();

        if (message != null) {
            Timer.Context time = Metrics.getTimer(Metrics.Type.NET_SERVER_PROCESS_REQUEST).time();

            ListenableFuture<Message> handle = messageHandler.handle(message);

            Futures.addCallback(handle, new FutureCallback<Message>() {
                @Override
                public void onSuccess(Message result) {
                    responseConsumer.accept(result);
                }

                @Override
                public void onFailure(Throwable t) {
                    LoggerFactory.getLogger(ServerWorker.class).warn("Cant process", t);

                    Message errorMessage = Message.createReply(message, StatusCode.SERVER_IS_NOT_AVAILABLE_FOR_OPERATIONS, ApiVersion.VERSION_1, new byte[0]);

                    responseConsumer.accept(errorMessage);
                }
            }, executorService);


            time.stop();
        }
    }

    @NotNull
    private ConcurrentLinkedQueue<Message> getNewMessages() {
        return newMessages;
    }
}
