package com.fnklabs.dds.network;

import com.fnklabs.metrics.MetricsFactory;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Handler for processing system messages from server (notifications) or messages that wasn't awaiting
 */
@Slf4j
@RequiredArgsConstructor
class NetworkClientWorker implements Runnable {
    private final Queue<Message> inputMessages;

    private final Consumer<Message> unboundMessageConsumer;

    private final Map<Long, ResponseFuture> responseFutureMap;

    private final AtomicBoolean isRunning;

    private final ExecutorService executorService;

    @Override
    public void run() {
        while (isRunning.get()) {
            Message message = inputMessages.poll();

            if (message != null) {
                log.debug("Received new message: {}", message);

                executorService.submit(() -> onNewMessage(message));
            }
        }
    }

    private void onNewMessage(Message message) {
        MetricsFactory.getMetrics().getCounter("network.client.worker.message").inc();

        ResponseFuture responseFuture = responseFutureMap.get(message.getReplyMessageId());

        if (responseFuture == null) {
            throw new RuntimeException(String.format("Message id `%d` doesn't exists", message.getReplyMessageId()));
//            unboundMessageConsumer.accept(message);
        }

        responseFuture.onResponse(message);
    }
}
