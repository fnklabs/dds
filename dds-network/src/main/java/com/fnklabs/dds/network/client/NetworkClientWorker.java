package com.fnklabs.dds.network.client;

import com.fnklabs.dds.network.ReplyMessage;
import com.fnklabs.dds.network.ResponseFuture;
import com.fnklabs.metrics.MetricsFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Handler for processing system messages from server (notifications) or messages that wasn't awaiting
 */
class NetworkClientWorker implements Runnable {
    private final static Logger log = LoggerFactory.getLogger(NetworkClientWorker.class);
    private final Queue<ReplyMessage> inputMessages;

    private final Consumer<ReplyMessage> unboundMessageConsumer;

    private final Map<Long, ResponseFuture> responseFutureMap;

    private final AtomicBoolean isRunning;

    NetworkClientWorker(Queue<ReplyMessage> inputMessages, Consumer<ReplyMessage> unboundMessageConsumer, Map<Long, ResponseFuture> responseFutureMap, AtomicBoolean isRunning) {
        this.inputMessages = inputMessages;
        this.unboundMessageConsumer = unboundMessageConsumer;
        this.responseFutureMap = responseFutureMap;
        this.isRunning = isRunning;
    }

    @Override
    public void run() {
        while (isRunning.get()) {
            ReplyMessage message = inputMessages.poll();

            if (message != null) {
                log.debug("Received new message: {}", message);

                onNewMessage(message);
            }
        }
    }

    private void onNewMessage(ReplyMessage message) {
        MetricsFactory.getMetrics().getCounter("network.client.worker.message").inc();

        ResponseFuture responseFuture = responseFutureMap.get(message.replyId());

        if (responseFuture == null) {
            throw new RuntimeException(String.format("BaseMessage id `%d` doesn't exists", message.replyId()));
//            unboundMessageConsumer.accept(message);
        }

        responseFuture.onResponse(message);
    }
}
