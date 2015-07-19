package com.fnklabs.dds.network.client;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Timer;
import com.fnklabs.dds.Metrics;
import com.fnklabs.dds.network.Message;
import com.fnklabs.dds.network.connector.Fragmentation;
import com.fnklabs.dds.network.connector.exception.HostNotAvailableException;
import com.google.common.net.HostAndPort;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.net.InetSocketAddress;
import java.net.StandardSocketOptions;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

class ClientConnector implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(ClientConnector.class);

    /**
     * Messages from server
     */
    private final ConcurrentLinkedQueue<Message> messagesFromServer = new ConcurrentLinkedQueue<>();

    /**
     * Remote address
     */
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
    private AtomicBoolean isRunning = new AtomicBoolean(true);
    /**
     * Incoming message messageBuffer
     */
    private ByteBuffer messageBuffer = ByteBuffer.allocate(Message.MAX_MESSAGE_SIZE);

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

    public ConcurrentLinkedQueue<Message> getMessagesFromServer() {
        return messagesFromServer;
    }

    @Override
    public void close() {
        isRunning.set(false);

        try {
            selector.close();
            channel.close();
        } catch (IOException e) {
            LOGGER.warn("Cant close socket", e);
        }

        LOGGER.info("Close connector: {}", remoteAddress);
    }

    /**
     * Send message to remote server
     *
     * @param data Data that must be sent
     */
    public void send(@NotNull ByteBuffer data) {
        Timer.Context timer = Metrics.getTimer(Metrics.Type.NET_CLIENT_SEND_MESSAGES).time();

        try {
            ByteBuffer message = ByteBuffer.allocate(data.limit() + Integer.BYTES);
            message.putInt(data.limit());
            message.put(data);

            message.rewind();

            int writtenBytes = write(message);

            LOGGER.debug("Send bytes: {}", writtenBytes);

            Counter counter = Metrics.getCounter(Metrics.Type.NET_CLIENT_SUCCESS_SEND_MESSAGES);
            counter.inc();
        } catch (HostNotAvailableException e) {
            Counter counter = Metrics.getCounter(Metrics.Type.NET_CLIENT_FAILED_SEND_MESSAGES);
            counter.inc();
        }

        timer.stop();
    }

    /**
     * Join thread pool for processing selector events
     *
     * @param executorService ExecutorService in which worker will be running
     */
    public void join(ExecutorService executorService) {
        executorService.submit(new ProcessSelectorEventsTask(this, executorService, isRunning));
    }


    /**
     * Process selector for retrieving messages from server
     */
    protected synchronized void processSelectorEvents() {
        try {
            if (!isRunning.get()) {
                LOGGER.warn("Connector to host {} is closed. Skip operation", remoteAddress);
            }

            if (!hasNewEvents()) {
                return;
            }

            Set<SelectionKey> keys = selector.selectedKeys();

            LOGGER.debug("New events: {}", keys.size());

            Metrics.getCounter(Metrics.Type.NET_SELECTOR_EVENTS).inc(keys.size());

            for (Iterator i = keys.iterator(); i.hasNext(); ) {
                Metrics.getCounter(Metrics.Type.NET_CLIENT_RETRIEVED_PACKETS).inc();

                SelectionKey key = (SelectionKey) i.next();
                i.remove();

                SocketChannel clientSocketChannel = (SocketChannel) key.channel();

                if (!key.isReadable()) {
                    continue;
                }

                while (true) {
                    /**
                     * read data from socket into the messageBuffer
                     */
                    int receivedBytes = clientSocketChannel.read(messageBuffer);

                    /**
                     * Check if no data in socket and nothing to read then break loop reading operation
                     */
                    if (receivedBytes == 0) {
                        break;
                    }

                    LOGGER.debug("{} New read bytes. received bytes: {}", channel.getLocalAddress(), receivedBytes);

                    /**
                     * Check if socket was closed
                     */
                    if (receivedBytes == -1) {
                        key.cancel();
                        clientSocketChannel.close();
                        continue;
                    }

                    int messageLength = Fragmentation.getMessageLength(messageBuffer);

                    if (messageLength <= messageBuffer.position() - Integer.BYTES) {

                        Metrics.getCounter(Metrics.Type.NET_CLIENT_RETRIEVED_MESSAGES).inc();

                        byte[] receivedData = Arrays.copyOfRange(messageBuffer.array(), Integer.BYTES, messageLength + Integer.BYTES);

                        if (messageLength < messageBuffer.position() - Integer.BYTES) {
                            byte[] data = Arrays.copyOfRange(messageBuffer.array(), messageLength + Integer.BYTES, messageBuffer.position());

                            messageBuffer.clear(); // free messageBuffer
                            messageBuffer.put(data); // add tail of buffer if there was data
                        } else {
                            messageBuffer.clear(); // free messageBuffer
                        }

                        /**
                         * Unserialize data into Message object
                         */
                        ObjectInputStream objectInputStream = new ObjectInputStream(new ByteArrayInputStream(receivedData));
                        Message o = (Message) objectInputStream.readObject();

                        LOGGER.debug("New messages from server ID: {} Reply ID: {} Status: {}", o.getId(), o.getReplyMessageId(), o.getStatusCode());
                        messagesFromServer.offer(o);
                    }
                }
            }

        } catch (Exception e) {
            LOGGER.warn("Cant process selector", e);
        }
    }

    /**
     * Check whether selector has new events
     *
     * @return true if selector has new events false otherwise
     *
     * @throws IOException
     */
    private boolean hasNewEvents() throws IOException {
        try {
            int select = selector.select(1);

            if (select > 0) {
                return true;
            }
        } catch (ClosedSelectorException e) {
            LOGGER.warn("Selector is closed.", e);
        }

        return false;
    }

    /**
     * Write data to channel
     *
     * @param data Data/message
     *
     * @throws HostNotAvailableException if cant send data
     */
    private int write(ByteBuffer data) throws HostNotAvailableException {
        Timer.Context timer = Metrics.getTimer(Metrics.Type.NET_WRITE_BYTES).time();

        int writtenData = 0;

        try {
            writtenData = channel.write(data);
            LoggerFactory.getLogger(getClass()).debug("Written data: {}", writtenData);
        } catch (IOException e) {
            LOGGER.warn("Cant write data to channel" + remoteAddress, e);

            throw new HostNotAvailableException("Cant write date", remoteAddress);
        } finally {
            timer.stop();
        }

        return writtenData;
    }


}
