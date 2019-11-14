package com.fnklabs.dds.network.client;

import com.fnklabs.concurrent.Executors;
import com.fnklabs.dds.network.ApiVersion;
import com.fnklabs.dds.network.ChannelClosedException;
import com.fnklabs.dds.network.Message;
import com.fnklabs.dds.network.ReplyMessage;
import com.fnklabs.dds.network.RequestException;
import com.fnklabs.dds.network.RequestMessage;
import com.fnklabs.dds.network.ResponseFuture;
import com.fnklabs.dds.network.pool.NetworkExecutor;
import com.fnklabs.metrics.MetricsFactory;
import com.fnklabs.metrics.Timer;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import static com.fnklabs.dds.network.NetworkConnector.*;


public class NetworkClient implements Closeable {
    private final static Logger log = LoggerFactory.getLogger(NetworkClient.class);
    /**
     * Is client running
     */
    private final AtomicBoolean isRunning = new AtomicBoolean(true);


    private final ExecutorService executorService;
    private final ScheduledExecutorService scheduler = Executors.scheduler(1, "network.client.wd");
    private final HostAndPort remoteAddress;
    /**
     * New (not process) messages from server for processing
     */
    private final Queue<ReplyMessage> messageQueue;
    private final ByteBuffer messageBuffer = ByteBuffer.allocate(ApiVersion.CURRENT.MAX_MESSAGE_SIZE);
    /**
     * Response futures map
     */
    private Map<Long, ResponseFuture> responseFutures = new ConcurrentHashMap<>();
    /**
     * Client channel
     */
    private SocketChannel channel;

    NetworkClient(HostAndPort remoteAddress, int workers, Consumer<ReplyMessage> unboundedMessageConsumer) throws IOException {
        this.remoteAddress = remoteAddress;

        messageQueue = new ArrayBlockingQueue<>(500);


//        connector = new NetworkClientConnector(
//                remoteAddress,
//                messageQueue,
//                Executors.fixedPoolExecutor(1, "network.client.io")
//        );

        executorService = Executors.fixedPoolExecutor(workers, "network.client.worker");

        executorService.submit(new NetworkClientWorker(messageQueue, unboundedMessageConsumer, responseFutures, isRunning));
        scheduler.scheduleWithFixedDelay(new RemovePendingRequestTask(responseFutures, isRunning), 0, 100, TimeUnit.MILLISECONDS);
    }

    public void join(NetworkExecutor executor) throws IOException {
        log.warn("Building client: {}", remoteAddress);

        channel = SocketChannel.open();
        channel.setOption(StandardSocketOptions.TCP_NODELAY, true);
        channel.configureBlocking(false);

        channel.connect(new InetSocketAddress(remoteAddress.getHost(), remoteAddress.getPort()));

        while (!channel.finishConnect()) {
            // await connect
        }

        executor.registerOpRead(channel, this::processSelectorEvents);
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

        write(channel, buffer);

        return responseFuture;
    }

    @Override
    public void close() throws IOException {
        isRunning.set(false);

        channel.close();


        executorService.shutdown();
        scheduler.shutdown();

        try {
            executorService.awaitTermination(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            log.warn("can't close client", e);
        }
        try {
            scheduler.awaitTermination(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            log.warn("can't close client", e);
        }
    }

    /**
     * Process selector for retrieving messages from server
     */
    private void processSelectorEvents(SelectionKey key) {

        SocketChannel clientSocketChannel = (SocketChannel) key.channel();

        if (!key.isReadable()) {
            return;
        }

        try {
            readFromChannel(clientSocketChannel, messageBuffer);

            messageBuffer.flip();

            readMessagesFromBuffer(messageBuffer, messageQueue::add);

            messageBuffer.compact();
        } catch (ChannelClosedException e) {
            closeChannel(key, clientSocketChannel);
        }


    }

    private void readMessagesFromBuffer(ByteBuffer messageBuffer, Consumer<ReplyMessage> newMessageHandler) {
        while (messageBuffer.remaining() >= Message.HEADER_SIZE) { // read all from buffer
            try (Timer timer = MetricsFactory.getMetrics().getTimer("network.client.connector.buffer.write")) {
                ReplyMessage message = new ReplyMessage();
                message.write(messageBuffer);

                newMessageHandler.accept(message);

                log.debug("Buffer: {}", messageBuffer);

            } catch (Exception e) {
                log.warn("can't write message from buffer", e);
            }
        }
    }


}
