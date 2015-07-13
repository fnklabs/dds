package com.fnklabs.dds.network.connector;

import com.codahale.metrics.Timer;
import com.fnklabs.dds.Metrics;
import com.fnklabs.dds.network.Packet;
import com.fnklabs.dds.network.StatusCode;
import com.fnklabs.dds.network.connector.exception.CantWriteException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class ServerConnector {
    private static final int MAX_REQUEST_BUFFER_SIZE = 32 * 1024;// 64 Kb
    private static final Logger LOGGER = LoggerFactory.getLogger(ServerConnector.class);
    /**
     * Client id sequence
     */
    private static final AtomicLong clientId = new AtomicLong(0);
    private static final int MAX_NEW_REQUESTS = 50000;
    private final String listenInterface;
    private final int listenPort;
    private final ExecutorService executorService;
    private final AtomicBoolean isRunning = new AtomicBoolean(false);
    private ServerSocketChannel serverSocketChannel;
    private SelectionKey selectionKey;
    private Selector selector;

    /**
     * new client buffer
     */
    private ConcurrentHashMap<Long, ByteBuffer> clientBuffers = new ConcurrentHashMap<>();

    /**
     * Active clients sockets channels
     */
    private ConcurrentHashMap<Long, SocketChannel> activeClients = new ConcurrentHashMap<>();

    /**
     * Pending request by message id
     */
    private ConcurrentHashMap<Long, MessageBuffer> pendingRequests = new ConcurrentHashMap<>();

    /**
     * New requests that are ready for processing
     */
    private ArrayBlockingQueue<MessageBuffer> newRequestBuffers = new ArrayBlockingQueue<>(MAX_NEW_REQUESTS);

    public ServerConnector(String listenInterface, int listenPort, ExecutorService executorService) {
        this.listenInterface = listenInterface;
        this.listenPort = listenPort;
        this.executorService = executorService;
    }

    public ArrayBlockingQueue<MessageBuffer> getNewRequestBuffers() {
        return newRequestBuffers;
    }

    /**
     * Join thread pool. Run process selector events loop
     */
    public void join() {
        executorService.submit(() -> {
            while (isRunning.get()) {
                processSelectorEvents();
            }
        });
    }

    public void sendMessageToClient(MessageBuffer data) {
        LOGGER.debug("Send message [{}] to client: {} data: {}", data.getId(), clientId, data.getMessageSize());

        Timer.Context time = Metrics.getTimer(Metrics.Type.NET_SERVER_SEND_MESSAGES).time();

        SocketChannel clientSocketChannel = getClientSocketChannel(data.getClient());

        AtomicInteger totalSendBytes = new AtomicInteger();

        Packet.splitToPacket(data.getResponseBuffer(), byteBuffer -> {
            while (byteBuffer.hasRemaining()) {

                try {
                    int write = clientSocketChannel.write(byteBuffer);

                    totalSendBytes.addAndGet(write);
                } catch (IOException e) {
                    LOGGER.warn("Cant write data to client", e);

                    throw new CantWriteException();
                }
            }

        });

        LOGGER.debug("Response [{}] was send to client [{}] {} bytes", data.getId(), clientId, totalSendBytes);

        time.stop();
    }

    public void create() throws IOException {
        LOGGER.info("Create server connector on {}:{}", listenInterface, listenPort);

        selector = Selector.open();

        serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.socket().bind(new InetSocketAddress(listenInterface, listenPort));
        serverSocketChannel.configureBlocking(false);

        selectionKey = serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);


        isRunning.set(true);
    }

    public void stop() {
        isRunning.set(false);

        try {
            serverSocketChannel.close();
            selector.close();
        } catch (IOException e) {
            LOGGER.warn("Cant close selector", e);
        }
    }

    private SocketChannel getClientSocketChannel(long clientId) {
        return activeClients.get(clientId);
    }

    /**
     * todo need refactoring
     */
    private void processSelectorEvents() {
        try {

            int select = selector.select();

            if (select == 0) {
                return;
            }

            Set keys = selector.selectedKeys();

            LOGGER.debug("New selector keys: {}/{}", select, keys.size());

            for (Iterator i = keys.iterator(); i.hasNext(); ) {
                Timer.Context time = Metrics.getTimer(Metrics.Type.NET_SERVER_PROCESS_SELECTOR).time();

                LOGGER.debug("New events: {}", keys.size());

                SelectionKey key = (SelectionKey) i.next();
                i.remove();

                if (key == selectionKey) {
                    if (key.isAcceptable()) {
                        SocketChannel clientChannel = serverSocketChannel.accept();

                        newConnectionEvent(clientChannel);
                    }
                } else {
                    SocketChannel clientSocketChannel = (SocketChannel) key.channel();

                    newReadEvent(key, clientSocketChannel);
                }

                time.stop();
            }
        } catch (Exception e) {
            LOGGER.warn("Cant process selector event", e);
        }
    }

    private void newReadEvent(SelectionKey key, SocketChannel clientSocketChannel) throws IOException {
        if (!key.isReadable())
            return;

        LOGGER.debug("New read event");

        Long clientId = (Long) key.attachment();


        ByteBuffer buffer = clientBuffers.get(clientId);

        if (buffer == null) {
            buffer = ByteBuffer.allocate(Packet.SIZE);

            clientBuffers.put(clientId, buffer);
        }

        try {
            int receivedBytes = clientSocketChannel.read(buffer);

            if (receivedBytes == -1) {
                key.cancel();
                clientSocketChannel.close();

                activeClients.remove(clientId);

                return;
            } else if (receivedBytes < Packet.SIZE) {
                return;
            }

            buffer.rewind();

            long id = Packet.getId(buffer);
            int sequence = Packet.getSequence(buffer);
            ByteBuffer data = Packet.getData(buffer);

            MessageBuffer requestBuffer = pendingRequests.get(id);

            if (requestBuffer == null) {
                requestBuffer = new MessageBuffer(clientId, getBuffer());
                pendingRequests.put(id, requestBuffer);
            }

            requestBuffer.append(data);

            if (requestBuffer.isFullyReceived()) {
                pendingRequests.remove(id);

                try {
                    newRequestBuffers.add(requestBuffer);
                } catch (IllegalStateException e) {
                    LOGGER.warn("Cant process request. server is busy now", e);

                    NetworkMessage<Boolean> message = new NetworkMessage<>();
                    message.setId(0);
                    message.setReplyMessageId(requestBuffer.getId());
                    message.setStatusCode(StatusCode.SERVER_IS_BUSY);

                    MessageBuffer messageBuffer = MessageUtils.transform(message);

                    sendMessageToClient(messageBuffer);
                }
            }

        } catch (IllegalArgumentException | IndexOutOfBoundsException e) {
            LOGGER.warn("Invalid request", e);
            pendingRequests.remove(clientId);

            NetworkMessage<Boolean> message = new NetworkMessage<>();
            message.setId(0);
            message.setReplyMessageId(-1);
            message.setStatusCode(StatusCode.SERVER_IS_BUSY);

            sendMessageToClient(MessageUtils.transform(message));
        }
    }

    private void newConnectionEvent(SocketChannel clientChannel) throws IOException {
        long nextId = getNextClientId();

        LOGGER.info("New connection");
        clientChannel.configureBlocking(false);
        clientChannel.setOption(StandardSocketOptions.TCP_NODELAY, true);
        SelectionKey clientKey = clientChannel.register(selector, SelectionKey.OP_READ);
        clientKey.attach(nextId);

        activeClients.put(nextId, clientChannel);
    }

    private long getNextClientId() {
        return clientId.incrementAndGet();
    }

    /**
     * Return Message buffer for specified message ID
     *
     * @return Buffer
     *
     * @throws IOException
     */
    private static ByteBuffer getBuffer() throws IOException {
        return ByteBuffer.allocate(MAX_REQUEST_BUFFER_SIZE);
    }
}
