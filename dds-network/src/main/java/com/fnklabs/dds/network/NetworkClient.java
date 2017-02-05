package com.fnklabs.dds.network;

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

    private final Queue<Message> messageQueue = new ConcurrentLinkedQueue<>();

    /**
     * Response futures map
     */
    private Map<Long, ResponseFuture> responseFutures = new ConcurrentHashMap<>();

    NetworkClient(HostAndPort remoteAddress, Consumer<Message> unboundedMessageConsumer) throws IOException {

        ThreadPoolExecutor executorService = getExecutorService();

        connector = new NetworkClientConnector(
                remoteAddress,
                messageQueue,
                executorService
        );

        join(executorService, unboundedMessageConsumer);
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
    }

    private void join(ExecutorService executorService, Consumer<Message> messageConsumer) {
        executorService.submit(new NetworkClientWorker(messageQueue, messageConsumer, responseFutures, isRunning));

        ScheduledExecutorService scheduledThreadPool = Executors.newScheduledThreadPool(1, new com.fnklabs.concurrent.ThreadFactory("network-client-wd"));
        scheduledThreadPool.scheduleWithFixedDelay(new RemovePendingRequestTask(responseFutures, isRunning), 0, 100, TimeUnit.MILLISECONDS);
    }

    private static ThreadPoolExecutor getExecutorService() {
        return new ThreadPoolExecutor(
                4,
                4,
                0L,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<Runnable>(500),
                new com.fnklabs.concurrent.ThreadFactory("network-client-worker"),
                new ThreadPoolExecutor.CallerRunsPolicy()
        );
    }
}
