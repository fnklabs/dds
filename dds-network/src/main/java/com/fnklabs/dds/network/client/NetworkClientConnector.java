package com.fnklabs.dds.network.client;

import com.fnklabs.dds.network.*;
import com.fnklabs.metrics.MetricsFactory;
import com.fnklabs.metrics.Timer;
import com.google.common.net.HostAndPort;
import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.SocketChannel;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

@Slf4j
class NetworkClientConnector extends NetworkConnector<ReplyMessage> implements Closeable {

    /**
     * New (not process) messages from server for processing
     */
    private final Queue<ReplyMessage> messageQueue;

    /**
     * Remote server address
     */
    private final HostAndPort remoteAddress;

    private final ExecutorService executorService;
    /**
     * Flag that determine whether client is running
     */
    private final AtomicBoolean isRunning = new AtomicBoolean(true);
    /**
     * Incoming messages messageBuffer
     */
    private final ByteBuffer messageBuffer = ByteBuffer.allocate(ApiVersion.CURRENT.MAX_MESSAGE_SIZE);
    /**
     * Client channel
     */
    private SocketChannel channel;

    NetworkClientConnector(HostAndPort remoteAddress, Queue<ReplyMessage> messageQueue, ExecutorService executorService) throws IOException {
        super(executorService, messageQueue);
        this.remoteAddress = remoteAddress;
        this.messageQueue = messageQueue;
        this.executorService = executorService;

        open();
    }

    @Override
    public void close() {
        isRunning.set(false);

        try {
            channel.close();
        } catch (IOException e) {
            log.warn("Cant close socket", e);
        }

        log.info("Close connector: {}", remoteAddress);

        executorService.shutdown();
    }

    /**
     * Send message to remote server
     *
     * @param data Data that must be sent
     */
    public void send(ByteBuffer data) {
        try {
            int writtenBytes = write(data);

            log.debug("send `{}` bytes", writtenBytes);

        } catch (HostNotAvailableException e) {
            log.error("Can't send data to server", e);
        }
    }

    @Override
    protected void readMessagesFromBuffer(ByteBuffer messageBuffer, Consumer<ReplyMessage> newMessageHandler) {
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

    /**
     * Process selector for retrieving messages from server
     */
    private void processSelectorEvents() {
        Set<SelectionKey> keys = selectNewEvents();

        for (SelectionKey key : keys) {
            SocketChannel clientSocketChannel = (SocketChannel) key.channel();

            if (!key.isReadable()) {
                continue;
            }

            try {
                readFromChannel(clientSocketChannel, messageBuffer);

                messageBuffer.flip();

                readMessagesFromBuffer(messageBuffer, messageQueue::add);

                messageBuffer.compact();
            } catch (ChannelClosedException e) {
                closeChannel(key, clientSocketChannel);
            }


            keys.remove(key);
        }
    }

    /**
     * Open socket connection to remote server
     *
     * @throws IOException
     */
    private void open() throws IOException {
        log.warn("Building client: {}", remoteAddress);

        channel = SocketChannel.open();
        channel.setOption(StandardSocketOptions.TCP_NODELAY, true);
        channel.configureBlocking(false);

        channel.connect(new InetSocketAddress(remoteAddress.getHostText(), remoteAddress.getPort()));

        while (!channel.finishConnect()) {
            // await connect
        }

        register(channel, SelectionKey.OP_READ);


        /**
         * Join thread pool for processing selector events (must be called only once)
         *
         * @param executorService ExecutorService in which worker will be running
         */
        executorService.submit(() -> {
            while (isRunning.get()) {
                processSelectorEvents();
            }
        });
    }

    /**
     * Write data to channel
     *
     * @param data Data/message
     *
     * @throws HostNotAvailableException if cant send data
     */
    private int write(ByteBuffer data) throws HostNotAvailableException {
        return write(channel, data);
    }
}
