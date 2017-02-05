package com.fnklabs.dds.network;

import com.fnklabs.concurrent.*;
import com.fnklabs.concurrent.Executors;
import com.google.common.base.Verify;
import com.google.common.net.HostAndPort;
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

    private final ExecutorService executorService = Executors.getThreadPoolExecutor(4, "network.client.worker");
    private final ScheduledExecutorService scheduler = Executors.scheduler(1, "network.client.wd");
    /**
     * Response futures map
     */
    private Map<Long, ResponseFuture> responseFutures = new ConcurrentHashMap<>();

    NetworkClient(HostAndPort remoteAddress, Consumer<Message> unboundedMessageConsumer) throws IOException {
        Queue<Message> messageQueue = new ArrayBlockingQueue<>(500);

        connector = new NetworkClientConnector(
                remoteAddress,
                messageQueue,
                Executors.getThreadPoolExecutor(1, "network.client.io")
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
        Message message = new Message(StatusCode.OK, ApiVersion.VERSION_1, data.array());

        log.debug("Sending message: {}", message);

        ByteBuffer msgData = Message.pack(message);

        Verify.verify(
                message.getMessageSize() == Message.messageLength(msgData),
                "Expected messages size: %s but was %s",
                message.getMessageSize(),
                Message.messageLength(msgData)
        );

        ResponseFuture responseFuture = new ResponseFuture(message);
        responseFutures.put(message.getId(), responseFuture);

        connector.send(msgData);

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
