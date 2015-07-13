package com.fnklabs.dds.network;

import com.codahale.metrics.Timer;
import com.fnklabs.dds.Metrics;
import com.fnklabs.dds.network.connector.Message;
import com.fnklabs.dds.network.connector.MessageBuffer;
import com.fnklabs.dds.network.connector.MessageUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

class ServerWorker implements Runnable {
    public static final Logger LOGGER = LoggerFactory.getLogger(ServerWorker.class);
    public static final int POLL_TIMEOUT = 5;

    /**
     * New request queue
     */
    private final ArrayBlockingQueue<MessageBuffer> requestBuffers;
    private final AtomicBoolean isRun;
    private final ContextHandler messageHandler;

    private final Consumer<MessageBuffer> responseConsumer;

    public ServerWorker(ArrayBlockingQueue<MessageBuffer> requestBuffers, AtomicBoolean isRun, ContextHandler messageHandler, Consumer<MessageBuffer> responseCallback) {
        this.requestBuffers = requestBuffers;
        this.isRun = isRun;
        this.messageHandler = messageHandler;
        this.responseConsumer = responseCallback;
    }

    @Override
    public void run() {
        LOGGER.debug("Starting worker...");

        while (isRun.get()) {
            process();
        }

        LOGGER.debug("Stop worker...");
    }

    /**
     *
     */
    protected void process() {
        try {
            MessageBuffer requestBuffer = getRequestBuffers().poll(POLL_TIMEOUT, TimeUnit.MILLISECONDS);

            if (requestBuffer != null) {
                Timer.Context time = Metrics.getTimer(Metrics.Type.NET_SERVER_PROCESS_REQUEST).time();
                processRequest(requestBuffer);
                time.stop();
            }
        } catch (InterruptedException e) {
            LOGGER.warn("Queue is empty. No new messages", e);
        }
    }


    private ArrayBlockingQueue<MessageBuffer> getRequestBuffers() {
        return requestBuffers;
    }

    private void processRequest(MessageBuffer requestBuffer) {
//        MessageHandler handler = messageHandler.getHandler(requestBuffer.getOperationCode());
//        Message<Object> transform = MessageUtils.transform(requestBuffer);
//        Message message = handler.handle(transform);

        responseConsumer.accept(requestBuffer);
    }
}
