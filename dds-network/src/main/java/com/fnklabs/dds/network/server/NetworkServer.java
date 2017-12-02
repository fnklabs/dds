package com.fnklabs.dds.network.server;

import com.fnklabs.concurrent.Executors;
import com.fnklabs.dds.network.*;
import com.fnklabs.dds.network.pool.NioServerExecutor;
import com.fnklabs.dds.network.pool.ServerExecutor;
import com.fnklabs.metrics.MetricsFactory;
import com.fnklabs.metrics.Timer;
import com.google.common.net.HostAndPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

public class NetworkServer implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(NetworkServer.class);

    private static final AtomicLong SESSION_ID_SEQUENCE = new AtomicLong();

    private final AtomicBoolean isRunning = new AtomicBoolean(true);

    /**
     * Local pool executor
     */
    private final ThreadPoolExecutor workerPoolExecutor;
    private final HostAndPort listenAddress;
    private final ServerExecutor executor;
    private final Map<Long, SocketChannel> sessions = new ConcurrentHashMap<>();
    /**
     * Client incoming message buffer
     */
    private final ConcurrentHashMap<Long, ByteBuffer> sessionBuffers = new ConcurrentHashMap<>();

    private final Queue<RequestMessage> incomeMessageQueue;
    private ServerSocketChannel serverSocketChannel;

    /**
     * Initialize network server but does not start it
     *
     * @param incomeMessageHandler New message handler
     */
    NetworkServer(HostAndPort listenAddress, int workers, IncomeMessageHandler incomeMessageHandler) throws IOException {

        executor = NioServerExecutor.builder().build();

        this.listenAddress = listenAddress;

        incomeMessageQueue = new ArrayBlockingQueue<>(10_000);

        this.workerPoolExecutor = Executors.fixedPoolExecutor(workers, "network.server.worker");

        workerPoolExecutor.submit(new NetworkServerWorker(incomeMessageQueue, isRunning, incomeMessageHandler, this::onNewReply));
    }

    public void run() throws IOException {
        serverSocketChannel = ServerSocketChannel.open();
        serverSocketChannel.socket().bind(new InetSocketAddress(listenAddress.getHost(), listenAddress.getPort()));
        serverSocketChannel.configureBlocking(false);

        SelectionKey selectionKey = executor.registerOpAccept(serverSocketChannel, key -> {
            if (key.isAcceptable()) {
                try {
                    SocketChannel accept = serverSocketChannel.accept();

                    onNewConnectionEvent(accept);
                } catch (IOException e) {
                    LOGGER.warn("can't accept key {}", key);
                }
            }
        });

        selectionKey.attach(-1); // stub for server

        executor.run();
    }

    @Override
    public void close() throws IOException {
        isRunning.set(false);

        executor.shutdown();
        workerPoolExecutor.shutdown();

        serverSocketChannel.close();
    }

    /**
     * Accept new connection and process it
     *
     * @param clientChannel New socket channel
     *
     * @throws IOException
     */
    private void onNewConnectionEvent(SocketChannel clientChannel) throws IOException {
        long sessionId = getNextSessionId();

        sessions.put(sessionId, clientChannel);

        LOGGER.info("new connection: {}", clientChannel.getRemoteAddress());

        clientChannel.configureBlocking(false);
        clientChannel.setOption(StandardSocketOptions.TCP_NODELAY, true);

        SelectionKey selectionKey = executor.registerOpRead(clientChannel, key -> {
            onReadEvent(key, clientChannel);
        });

        selectionKey.attach(sessionId);

        selectionKey = executor.registerOpWrite(clientChannel, key -> {

        });

        selectionKey.attach(sessionId);
    }

    /**
     * Process new write events from socket channel
     *
     * @param key                 Selection key
     * @param clientSocketChannel Socket channel
     */
    private void onReadEvent(SelectionKey key, SocketChannel clientSocketChannel) {
        if (!key.isReadable())
            return;

        long sessionId = (long) key.attachment();

        LOGGER.debug("new read event from client: {}", sessionId);

        ByteBuffer messageBuffer = sessionBuffers.computeIfAbsent(sessionId, id -> ByteBuffer.allocate(ApiVersion.CURRENT.MAX_MESSAGE_SIZE));

        try {
            NetworkConnector.readFromChannel(clientSocketChannel, messageBuffer);
        } catch (ChannelClosedException e) {
            onChannelClose(key, clientSocketChannel);

            return;
        }

        messageBuffer.flip();

        readMessagesFromBuffer(messageBuffer, message -> {
            message.setSessionId(sessionId);

            if (!incomeMessageQueue.add(message)) {
                throw new RuntimeException(new MessageQueueLimits(message));
            }
        });


        messageBuffer.compact();
    }

    private void readMessagesFromBuffer(ByteBuffer messageBuffer, Consumer<RequestMessage> newMessageHandler) {
        while (messageBuffer.remaining() >= Message.HEADER_SIZE) {
            try (Timer timer = MetricsFactory.getMetrics().getTimer("network.server.connector.buffer.read")) {
                RequestMessage message = new RequestMessage();
                message.write(messageBuffer);

                newMessageHandler.accept(message);

                LOGGER.debug("new message: {}", message);

            } catch (Exception e) {
                LOGGER.warn("can't write message from buffer", e);
            }
        }
    }

    private void onChannelClose(SelectionKey key, SelectableChannel clientSocketChannel) {
        Long clientId = (Long) key.attachment();

        sessions.remove(clientId);
        sessionBuffers.remove(clientId);

        NetworkConnector.closeChannel(key, clientSocketChannel);
    }

    /**
     * Return next client id from sequence
     *
     * @return Next client id
     */
    private long getNextSessionId() {
        return SESSION_ID_SEQUENCE.incrementAndGet();
    }

    /**
     * Send message to remote session
     *
     * @param session Client id
     * @param data    Data
     */
    private int sendMessage(long session, ByteBuffer data) throws Exception {
        try (Timer timer = MetricsFactory.getMetrics().getTimer("network.server.connector.write")) {
            LOGGER.debug("Send message to session: {} data size: {}", session, data.limit());

            SocketChannel clientSocketChannel = sessions.get(session);

            if (clientSocketChannel == null) {
                LOGGER.warn("Socket channel was already closed: {}", session);
                return 0;
            }

            int writtenData = NetworkConnector.write(clientSocketChannel, data);

            LOGGER.debug("Response was send to session [{}] total send bytes: {} ", session, writtenData);

            return writtenData;
        }
    }

    private void onNewReply(long sessionId, ReplyMessage message) {
        LOGGER.debug("Receive reply message: {}", message);

        try (Timer timer = MetricsFactory.getMetrics().getTimer("network.server.reply")) {
            ByteBuffer buffer = ByteBuffer.allocate(message.getSize());
            message.read(buffer);
            buffer.rewind();

            sendMessage(sessionId, buffer);
        } catch (Exception e) {
            LOGGER.warn("Can't send data to client", e);
        }

        LOGGER.debug("message with id: {} was send with status: {}", message.getId(), message.getStatusCode());
    }
}