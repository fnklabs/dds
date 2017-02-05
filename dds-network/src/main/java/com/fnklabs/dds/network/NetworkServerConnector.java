package com.fnklabs.dds.network;

import com.fnklabs.metrics.MetricsFactory;
import com.fnklabs.metrics.Timer;
import com.google.common.net.HostAndPort;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

class NetworkServerConnector extends NetworkConnector implements Closeable {
    private static final long CLIENT_SERVER_ID = -1;
    private static final Logger LOGGER = LoggerFactory.getLogger(NetworkServerConnector.class);
    /**
     * Client id sequence
     */
    private static final AtomicLong CLIENT_ID_SEQUENCE = new AtomicLong(0);

    private final AtomicBoolean isRunning = new AtomicBoolean(false);

    private final HostAndPort listenInterfaceAndPort;

    private final ThreadPoolExecutor executorService;

    /**
     * Client incoming message buffer
     */
    private final ConcurrentHashMap<Long, ByteBuffer> clientBuffers = new ConcurrentHashMap<>();
    /**
     * Active clients sockets channels
     */
    private final ConcurrentHashMap<Long, SocketChannel> clientChannels = new ConcurrentHashMap<>();
    /**
     * New request messages that are ready for processing
     */
    private final Queue<Message> newRequestMessages;

    private ServerSocketChannel serverSocketChannel;

    NetworkServerConnector(HostAndPort listenInterfaceAndPort, ThreadPoolExecutor executorService, int selectors, Queue<Message> newRequestMessages) throws
            IOException {
        super(executorService, newRequestMessages);
        this.listenInterfaceAndPort = listenInterfaceAndPort;
        this.executorService = executorService;
        this.newRequestMessages = newRequestMessages;

        LOGGER.info("Create server connector on {}", listenInterfaceAndPort);

        serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.socket().bind(new InetSocketAddress(listenInterfaceAndPort.getHost(), listenInterfaceAndPort.getPort()));
        serverSocketChannel.configureBlocking(false);

        SelectionKey selectionKey = register(serverSocketChannel, SelectionKey.OP_ACCEPT);
        selectionKey.attach(CLIENT_SERVER_ID);

        isRunning.set(true);

        for (int i = 0; i < selectors; i++) {
            executorService.submit(() -> {
                while (isRunning.get()) {
                    processSelectorEvents();
                }
            });
        }
    }

    /**
     * Send message to remote client
     *
     * @param client Client id
     * @param data   Data
     */
    int sendMessageToClient(long client, ByteBuffer data) {
        LOGGER.debug("Send message to client: {} data size: {}", client, data.limit());

        SocketChannel clientSocketChannel = getClientSocketChannel(client);

        if (clientSocketChannel == null) {
            LOGGER.warn("Socket channel was already closed: {}", client);
            return 0;
        }

        int writtenData = write(clientSocketChannel, data);

        LOGGER.debug("Response was send to client [{}] total send bytes: {} ", client, writtenData);

        return writtenData;
    }

    @Override
    public void close() {
        isRunning.set(false);

        try {
            serverSocketChannel.close();
        } catch (IOException e) {
            LOGGER.warn("Cant close selector", e);
        }
    }

    /**
     * todo need refactoring
     */
    private void processSelectorEvents() {
        try {
            Set<SelectionKey> keys = selectNewEvents();

            for (SelectionKey key : keys) {
                if ((Long) key.attachment() == CLIENT_SERVER_ID) {
                    if (key.isAcceptable()) {
                        onNewConnectionEvent(serverSocketChannel.accept());
                    }
                } else {
                    SocketChannel clientSocketChannel = (SocketChannel) key.channel();

                    synchronized (key.attachment()) {
                        try (Timer timer = MetricsFactory.getMetrics().getTimer("network.server.event.read")) {
                            onReadEvent(key, clientSocketChannel);
                        }
                    }
                }

                keys.remove(key);
            }
        } catch (Exception e) {
            LOGGER.warn("Can't process selector event", e);
        }
    }


    @Nullable
    private SocketChannel getClientSocketChannel(long clientId) {
        return clientChannels.get(clientId);
    }


    /**
     * Process new read events from socket channel
     *
     * @param key                 Selection key
     * @param clientSocketChannel Socket channel
     */
    private void onReadEvent(SelectionKey key, SocketChannel clientSocketChannel) throws MessageQueueLimits {
        if (!key.isReadable())
            return;

        long clientId = (long) key.attachment();

        LOGGER.debug("New read event from client: {}", clientId);

        ByteBuffer messageBuffer = clientBuffers.computeIfAbsent(clientId, clientKey -> ByteBuffer.allocate(Message.MAX_MESSAGE_SIZE));

        try {
            readFromChannel(clientSocketChannel, messageBuffer);
        } catch (ChannelClosedException e) {
            onChannelClose(key, clientSocketChannel);
        }

        readMessagesFromBuffer(messageBuffer, message -> {
            message.setClient(clientId);

            if (!newRequestMessages.add(message)) {
                throw new RuntimeException(new MessageQueueLimits(message));
            }
        });
    }

    private void onChannelClose(SelectionKey key, SocketChannel clientSocketChannel) {
        Long clientId = (Long) key.attachment();

        clientChannels.remove(clientId);
        clientBuffers.remove(clientId);

        closeChannel(key, clientSocketChannel);
    }

    /**
     * Accept new connection and process it
     *
     * @param clientChannel New socket channel
     *
     * @throws IOException
     */
    private void onNewConnectionEvent(SocketChannel clientChannel) throws IOException {
        long clientId = getNextClientId();

        LOGGER.info("New client connection: {}", clientChannel.getRemoteAddress());

        clientChannel.configureBlocking(false);
        clientChannel.setOption(StandardSocketOptions.TCP_NODELAY, true);

        SelectionKey selectionKey = register(clientChannel, SelectionKey.OP_READ);
        selectionKey.attach(clientId);

        clientChannels.put(clientId, clientChannel);
    }

    /**
     * Return next client id from sequence
     *
     * @return Next client id
     */
    private long getNextClientId() {
        return CLIENT_ID_SEQUENCE.incrementAndGet();
    }
}
