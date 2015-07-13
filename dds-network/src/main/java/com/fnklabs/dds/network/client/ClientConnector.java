package com.fnklabs.dds.network.client;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;
import com.fnklabs.dds.Metrics;
import com.fnklabs.dds.network.Operation;
import com.fnklabs.dds.network.Packet;
import com.fnklabs.dds.network.ResponseFuture;
import com.fnklabs.dds.network.client.exception.RemoveTimeoutRequest;
import com.fnklabs.dds.network.connector.exception.HostNotAvailableException;
import com.fnklabs.dds.network.connector.MessageBuffer;
import com.fnklabs.dds.network.connector.MessageUtils;
import com.fnklabs.dds.network.connector.NetworkMessage;
import com.google.common.net.HostAndPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.Serializable;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

class ClientConnector {
    public static final Logger LOGGER = LoggerFactory.getLogger(ClientConnector.class);
    private static final int MSG_BUFFER_SIZE = 64 * 1024; // 64 KB
    private static final int REQUEST_TIMEOUT = 5000;
    private static final AtomicInteger RETRIEVED_SELECTOR_EVENTS = new AtomicInteger(0);
    private static final int QUEUE_CAPACITY = 5000;
    private static final int AWAIT_TERMINATION_SECONDS = 10;
    private static final int BUFFER_SIZE = Packet.SIZE;

    private final AtomicLong ID_SEQUENCE = new AtomicLong(1);

    /**
     * Queue of notification messages or messages that are not linked with local response futures by Message id
     */
    private final ArrayBlockingQueue<MessageBuffer> notificationMessages = new ArrayBlockingQueue<>(QUEUE_CAPACITY);

    /**
     * Waiting response by message id
     */
    private final ConcurrentHashMap<Long, ResponseFuture> responseFutures = new ConcurrentHashMap<>();

    /**
     * Pending response by packet id
     */
    private final ConcurrentHashMap<Long, MessageBuffer> pendingResponse = new ConcurrentHashMap<>();

    /**
     * Data buffer
     */
    private final ByteBuffer buffer = ByteBuffer.allocate(BUFFER_SIZE);

    private HostAndPort remoteAddress;

    /**
     * Client channel
     */
    private SocketChannel channel;

    /**
     * Selector
     */
    private Selector selector;

    /**
     *
     */
    private AtomicLong retrievedPackets = new AtomicLong(0);

    /**
     *
     */
    private AtomicBoolean isActive = new AtomicBoolean(false);

    private ClientConnector() {

    }

    /**
     * Build client instance
     *
     * @return Client instance
     *
     * @throws IOException
     */
    public static ClientConnector build(HostAndPort remoteAddress) throws IOException {
        ClientConnector clientConnector = new ClientConnector();
        clientConnector.remoteAddress = remoteAddress;
        LOGGER.warn("Building client: {}:{}", remoteAddress.getHostText(), remoteAddress.getPort());

        clientConnector.channel = SocketChannel.open();

        clientConnector.channel.setOption(StandardSocketOptions.TCP_NODELAY, true);
        clientConnector.channel.configureBlocking(false);
        boolean connect = clientConnector.channel.connect(new InetSocketAddress(remoteAddress.getHostText(), remoteAddress.getPort()));

        if (!connect) {
//            throw new IOException("Can't connect to remote host: " + remoteAddress);
        }

        while (!clientConnector.channel.finishConnect()) {

        }

        clientConnector.selector = Selector.open();
        clientConnector.channel.register(clientConnector.selector, SelectionKey.OP_READ);

        return clientConnector;
    }

    public ArrayBlockingQueue<MessageBuffer> getNotificationMessages() {
        return notificationMessages;
    }

    public void close() {
        isActive.set(false);
        try {
            selector.close();
            channel.close();
        } catch (IOException e) {
            LOGGER.warn("Cant close socket", e);
        }

        LOGGER.info("Close connector: {}", remoteAddress);
    }

    public <T extends Serializable> ResponseFuture send(Operation operation, T data) {
        Timer.Context timer = Metrics.getTimer(Metrics.Type.NET_CLIENT_SEND_MESSAGES).time();

        ResponseFuture responseFuture = new ResponseFuture();


        long id = getIdSequence();

        NetworkMessage<T> networkMessage = new NetworkMessage<>();
        networkMessage.setOperationCode(operation.getCode());
        networkMessage.setId(id);
        networkMessage.setData(data);

        MessageBuffer transform = MessageUtils.transform(networkMessage);

        try {
            Packet.splitToPacket(transform.getResponseBuffer(), splittedData -> {
                try {
                    write(splittedData);
                } catch (HostNotAvailableException e) {
                    LOGGER.warn("Can't write data", e);

                    throw e;
                }
            });

            Counter counter = Metrics.getCounter(Metrics.Type.NET_CLIENT_SUCCESS_SEND_MESSAGES);
            counter.inc();
        } catch (HostNotAvailableException e) {
            Counter counter = Metrics.getCounter(Metrics.Type.NET_CLIENT_FAILED_SEND_MESSAGES);
            counter.inc();
        }


        responseFutures.put(id, responseFuture);

        timer.stop();

        return responseFuture;
    }

    public void join(ExecutorService executorService) {
        executorService.submit(new ProcessSelectorEventsHandler(this, executorService));

        executorService.submit(new RemoveTimeoutRequest(executorService, responseFutures));
    }

    protected long getIdSequence() {
        return ID_SEQUENCE.getAndIncrement();
    }

    protected synchronized void processSelectorEvents() {
        try {
            if (!isActive.get()) {
                LOGGER.warn("Connector to host {} is closed. Skip operation", remoteAddress);
            }

            try {
                int select = selector.select(1);
                if (select == 0) {
                    return;
                }
            } catch (ClosedSelectorException e) {
                LOGGER.warn("Selector is closed.", e);

                return;
            }

            RETRIEVED_SELECTOR_EVENTS.getAndIncrement();

            Set<SelectionKey> keys = selector.selectedKeys();

            for (Iterator i = keys.iterator(); i.hasNext(); ) {
                retrievedPackets.getAndIncrement();

//                LOGGER.debug("New events: {}", keys.size());

                SelectionKey key = (SelectionKey) i.next();
                i.remove();


                SocketChannel clientSocketChannel = (SocketChannel) key.channel();
                if (!key.isReadable())
                    continue;

                try {
                    while (true) {
                        int receivedBytes = clientSocketChannel.read(buffer);

                        if (receivedBytes == 0) {
                            break;
                        }

                        LOGGER.debug("{} New read bytes. received bytes: {}", channel.getLocalAddress(), receivedBytes);

                        if (receivedBytes == -1) {
                            key.cancel();
                            clientSocketChannel.close();
                            continue;
                        }

                        if (receivedBytes < BUFFER_SIZE) {
                            continue;
                        }

                        buffer.rewind();

                        long packetId = Packet.getId(buffer);
                        int sequenceId = Packet.getSequence(buffer);
                        ByteBuffer data = Packet.getData(buffer);


                        MessageBuffer connectorMessageBuffer = pendingResponse.get(packetId);

                        if (connectorMessageBuffer == null) {
                            Metrics.getCounter(Metrics.Type.NET_CLIENT_RETRIEVED_MESSAGES).inc();
                            connectorMessageBuffer = new MessageBuffer(0, getBuffer());
                            pendingResponse.put(packetId, connectorMessageBuffer);
                        }

                        connectorMessageBuffer.append(data);

                        if (connectorMessageBuffer.isFullyReceived()) {

                            pendingResponse.remove(packetId);

                            if (responseFutures.containsKey(connectorMessageBuffer.getId())) {

                                ResponseFuture responseFuture = responseFutures.get(connectorMessageBuffer.getId());
                                responseFuture.onResponse(connectorMessageBuffer);

                                responseFutures.remove(connectorMessageBuffer.getId());
                            } else {
                                notificationMessages.offer(connectorMessageBuffer);
                            }

                        }

                        buffer.clear();
                    }

                } catch (Exception e) {
                    LOGGER.warn("Invalid response", e);
                }
            }

        } catch (Exception e) {
            LOGGER.warn("Cant process selector", e);
        }
    }

    /**
     * Write data to channel
     *
     * @param data Data/message
     *
     * @throws HostNotAvailableException if cant send data
     */
    private void write(ByteBuffer data) throws HostNotAvailableException {
        Timer.Context timer = Metrics.getTimer(Metrics.Type.NET_WRITE_BYTES).time();
        try {
            while (data.hasRemaining()) {
                int numBytesWritten = channel.write(data);
                LoggerFactory.getLogger(getClass()).debug("Written data: {}", numBytesWritten);
            }
        } catch (IOException e) {
            LOGGER.warn("Cant write data to channel" + remoteAddress, e);

            throw new HostNotAvailableException("Cant write date", remoteAddress);
        } finally {
            timer.stop();
        }
    }

    /**
     * Return Message buffer for specified message ID
     *
     * @return Buffer
     *
     * @throws IOException
     */
    private ByteBuffer getBuffer() throws IOException {
        return ByteBuffer.allocate(MSG_BUFFER_SIZE);
    }


}
