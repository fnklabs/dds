package com.fnklabs.dds.network.client;

import com.fnklabs.concurrent.Executors;
import com.fnklabs.dds.network.*;
import com.fnklabs.metrics.MetricsFactory;
import com.fnklabs.metrics.Timer;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

@Slf4j
public class NetworkClient implements Closeable {

    /**
     * Is client running
     */
    private final AtomicBoolean isRunning = new AtomicBoolean(true);
    /**
     * Client connector instance
     */
    private final NetworkClientConnector connector;

    private final ExecutorService executorService = Executors.fixedPoolExecutor(4, "network.client.worker");
    private final ScheduledExecutorService scheduler = Executors.scheduler(1, "network.client.wd");
    /**
     * Response futures map
     */
    private Map<Long, ResponseFuture> responseFutures = new ConcurrentHashMap<>();

    NetworkClient(HostAndPort remoteAddress, Consumer<ReplyMessage> unboundedMessageConsumer) throws IOException {
        Queue<ReplyMessage> messageQueue = new ArrayBlockingQueue<>(500);

        connector = new NetworkClientConnector(
                remoteAddress,
                messageQueue,
                Executors.fixedPoolExecutor(1, "network.client.io")
        );


        executorService.submit(new NetworkClientWorker(messageQueue, unboundedMessageConsumer, responseFutures, isRunning, executorService));
        scheduler.scheduleWithFixedDelay(new RemovePendingRequestTask(responseFutures, isRunning), 0, 100, TimeUnit.MILLISECONDS);
    }

    /**
     * Send message
     *
     * @throws RequestException if send request message
     */
    public ResponseFuture send(ByteBuffer data) throws RequestException {
        Timer timer = MetricsFactory.getMetrics().getTimer("network.client.send");

        RequestMessage message = new RequestMessage(RequestMessage.ID.incrementAndGet(), ApiVersion.CURRENT, data.array());

        log.debug("Sending message: {}", message);

        ByteBuffer buffer = ByteBuffer.allocate(message.getSize());

        message.read(buffer);

        buffer.rewind();

        ResponseFuture responseFuture = new ResponseFuture(message);

        Futures.addCallback(responseFuture, new FutureCallback<ReplyMessage>() {
            @Override
            public void onSuccess(ReplyMessage result) {
                timer.stop();
            }

            @Override
            public void onFailure(Throwable t) {
                timer.stop();
            }
        });
        responseFutures.put(message.getId(), responseFuture);

        connector.send(buffer);

        return responseFuture;
    }

    @Override
    public void close() {
        connector.close();

        isRunning.set(false);


        executorService.shutdown();
        scheduler.shutdown();
    }


}
