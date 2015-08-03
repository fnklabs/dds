package com.fnklabs.dds.network.server;

import com.codahale.metrics.Timer;
import com.fnklabs.dds.Metrics;
import com.fnklabs.dds.network.ApiVersion;
import com.fnklabs.dds.network.Message;
import com.fnklabs.dds.network.StatusCode;
import com.fnklabs.dds.network.connector.Fragmentation;
import com.fnklabs.dds.network.connector.exception.CantWriteException;
import com.google.common.net.HostAndPort;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

class ServerConnector implements Closeable {
    private static final Logger LOGGER = LoggerFactory.getLogger(ServerConnector.class);
    /**
     * Client id sequence
     */
    private static final AtomicLong clientId = new AtomicLong(0);

    private final AtomicBoolean isRunning = new AtomicBoolean(false);

    private final HostAndPort listenInterfaceAndPort;
    /**
     * Client incoming message buffer
     */
    private final ConcurrentHashMap<Long, ByteBuffer> clientBuffers = new ConcurrentHashMap<>();
    /**
     * Active clients sockets channels
     */
    private final ConcurrentHashMap<Long, SocketChannel> activeClients = new ConcurrentHashMap<>();
    /**
     * New request messages that are ready for processing
     */
    private final ConcurrentLinkedQueue<Message> newRequestMessages = new ConcurrentLinkedQueue<>();

    private ServerSocketChannel serverSocketChannel;
    private SelectionKey selectionKey;
    private Selector selector;

    /**
     * @param listenInterfaceAndPort
     */
    public ServerConnector(HostAndPort listenInterfaceAndPort) {
        this.listenInterfaceAndPort = listenInterfaceAndPort;
    }

    public HostAndPort getListenInterfaceAndPort() {
        return listenInterfaceAndPort;
    }

    /**
     * Send message to remote client
     *
     * @param client Client id
     * @param data   Data
     */
    public void sendMessageToClient(long client, ByteBuffer data) {
        LOGGER.debug("Send message to client: {} data size: {}", client, data.limit());

        Timer.Context time = Metrics.getTimer(Metrics.Type.NET_SERVER_SEND_MESSAGES).time();

        SocketChannel clientSocketChannel = getClientSocketChannel(client);

        AtomicInteger totalSendBytes = new AtomicInteger();

        if (clientSocketChannel == null) {
            LOGGER.warn("Socket channel was already closed: {}", client);
            return;
        }

        try {
            ByteBuffer msg = ByteBuffer.allocate(data.limit() + Integer.BYTES);
            msg.putInt(data.limit());
            msg.put(data);

            msg.rewind();

            int write = clientSocketChannel.write(msg);

            totalSendBytes.addAndGet(write);
        } catch (IOException e) {
            LOGGER.warn("Cant write data to client", e);

            throw new CantWriteException();
        }


        LOGGER.debug("Response was send to client [{}] total send bytes: {} ", client, totalSendBytes);

        time.stop();
    }

    @Override
    public void close() {
        isRunning.set(false);

        try {
            serverSocketChannel.close();
            selector.close();
        } catch (IOException e) {
            LOGGER.warn("Cant close selector", e);
        }
    }

    protected void create() throws IOException {
        LOGGER.info("Create server connector on {}", listenInterfaceAndPort);

        selector = Selector.open();

        serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.socket().bind(new InetSocketAddress(listenInterfaceAndPort.getHostText(), listenInterfaceAndPort.getPort()));
        serverSocketChannel.configureBlocking(false);

        selectionKey = serverSocketChannel.register(selector, SelectionKey.OP_ACCEPT);


        isRunning.set(true);
    }

    /**
     * Join thread pool. Run process selector events loop
     */
    protected void join(ExecutorService executorService) {
        executorService.submit(new SelectorProcessingTask(executorService, this, isRunning));
    }

    protected ConcurrentLinkedQueue<Message> getNewRequestMessages() {
        return newRequestMessages;
    }

    /**
     * todo need refactoring
     */
    protected void processSelectorEvents() {
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

                    onReadEvent(key, clientSocketChannel);
                }

                time.stop();
            }
        } catch (Exception e) {
            LOGGER.warn("Can't process selector event", e);
        }
    }

    @Nullable
    private SocketChannel getClientSocketChannel(long clientId) {
        return activeClients.get(clientId);
    }

    /**
     * Process new read events from socket channel
     *
     * @param key                 Selection key
     * @param clientSocketChannel Socket channel
     *
     * @throws IOException
     */
    private void onReadEvent(SelectionKey key, SocketChannel clientSocketChannel) throws IOException {
        if (!key.isReadable())
            return;

        Long clientId = (Long) key.attachment();

        LOGGER.debug("New read event from client: {}", clientId);

        ByteBuffer buffer = clientBuffers.computeIfAbsent(clientId, clientKey -> {
            return ByteBuffer.allocate(Message.MAX_MESSAGE_SIZE);
        });

        try {
            int receivedBytes = clientSocketChannel.read(buffer);

            LOGGER.debug("Received bytes: {} from {}", receivedBytes, clientId);

            /**
             * Check of socket was closed
             */
            if (receivedBytes == -1) {
                key.cancel();
                clientSocketChannel.close();

                activeClients.remove(clientId);
                clientBuffers.remove(clientId);

                return;
            } else if (receivedBytes == 0) {
                return;
            }


            int messageLength = Fragmentation.getMessageLength(buffer);

            if (messageLength <= buffer.position() - Integer.BYTES) {

                Metrics.getCounter(Metrics.Type.NET_CLIENT_RETRIEVED_MESSAGES).inc();

                byte[] receivedData = Arrays.copyOfRange(buffer.array(), Integer.BYTES, messageLength + Integer.BYTES);

                if (messageLength < buffer.position() - Integer.BYTES) {
                    byte[] data = Arrays.copyOfRange(buffer.array(), messageLength + Integer.BYTES, buffer.position());

                    buffer.clear(); // free messageBuffer
                    buffer.put(data); // add tail of buffer if there was data
                } else {
                    buffer.clear();
                }

                /**
                 * Unserialize data into Message object
                 */
                ObjectInputStream objectInputStream = new ObjectInputStream(new ByteArrayInputStream(receivedData));
                Message o = (Message) objectInputStream.readObject();
                o.setClient(clientId);

                newRequestMessages.offer(o);
            }

        } catch (IllegalArgumentException | IndexOutOfBoundsException | ClassNotFoundException e) {
            LOGGER.warn("Invalid request", e);


            Message msg = new Message(Message.getNextId(), -1, StatusCode.UNKNOWN, ApiVersion.VERSION_1, new byte[0], clientId);

            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(out);
            objectOutputStream.writeObject(msg);

            byte[] bytes = out.toByteArray();

            sendMessageToClient(clientId, ByteBuffer.wrap(bytes));
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
}
