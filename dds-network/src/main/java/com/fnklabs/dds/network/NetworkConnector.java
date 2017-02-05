package com.fnklabs.dds.network;

import com.fnklabs.metrics.MetricsFactory;
import com.fnklabs.metrics.Timer;
import com.google.common.base.Verify;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.AbstractSelectableChannel;
import java.util.Collections;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

@Slf4j
abstract class NetworkConnector {
    private final Selector selector = Selector.open();

    private final Queue<Message> messageQueue;

    private final ExecutorService executorService;

    NetworkConnector(ExecutorService executorService, Queue<Message> messageQueue) throws IOException {
        this.executorService = executorService;
        this.messageQueue = messageQueue;
    }

    /**
     * Register provided channel into selector
     *
     * @param channel   SocketChannel
     * @param operation Socket operation
     *
     * @return
     */
    SelectionKey register(AbstractSelectableChannel channel, int operation) throws ClosedChannelException {
        return channel.register(selector, operation);
    }

    void readMessagesFromBuffer(ByteBuffer messageBuffer, Consumer<Message> newMessageHandler) {
        for (; ; ) {
            try (Timer timer = MetricsFactory.getMetrics().getTimer("network.connector.buffer.read")) {
                int messageLength = Message.messageLength(messageBuffer);

                 /*  If all data was read */
                int messageBufferCurrentPosition = messageBuffer.position();

                if (messageLength <= messageBufferCurrentPosition) {
                    messageBuffer.position(0);

                    Message clientMessage = Message.unpack(messageBuffer);

                    if (messageLength < messageBufferCurrentPosition) { // copy next message data
                        for (int i = messageLength; i < messageBufferCurrentPosition; i++) {
                            int shiftIndex = i - messageLength;
                            messageBuffer.put(shiftIndex, messageBuffer.get(i));
                        }

                        messageBuffer.position(messageBufferCurrentPosition - messageLength);
                    }


                    newMessageHandler.accept(clientMessage);
                } else {
                    break;
                }
            } catch (Exception e) {
                log.warn("can't read message from buffer", e);
            }
        }
    }

    void closeChannel(SelectionKey key, SocketChannel clientSocketChannel) {
        try {
            key.cancel();
            clientSocketChannel.close();
        } catch (IOException e) {
            log.warn("Can't close channel", e);
        }
    }

    Set<SelectionKey> selectNewEvents() {
        try {
            int select = selector.select();

            if (select != 0) {
                Set<SelectionKey> keys = selector.selectedKeys();

                log.debug("New selector keys: {}/{}", select, keys.size());

                return keys;
            }
        } catch (IOException e) {
            log.error("Cant select keys", e);
        }

        return Collections.emptySet();
    }

    protected void readFromChannel(SocketChannel socketChannel, ByteBuffer buffer) throws ChannelClosedException {
        try (Timer timer = MetricsFactory.getMetrics().getTimer("network.connector.channel.read")) {
            int receivedBytes = socketChannel.read(buffer);

            MetricsFactory.getMetrics().getCounter("network.connector.channel.in").inc(receivedBytes);

            log.debug("received `{}` bytes: from {}", receivedBytes, socketChannel.getRemoteAddress());

            /* Check if socket was closed */
            if (receivedBytes == -1) {
                throw new ChannelClosedException(socketChannel);
            }
        } catch (IOException e) {
            log.error("Can't read from channel {}/{}", socketChannel, e);
        }

    }


    /**
     * Write data to channel
     *
     * @param data Data/message
     *
     * @throws HostNotAvailableException if cant send data
     */
    protected int write(SocketChannel socketChannel, ByteBuffer data) throws HostNotAvailableException {

        try (Timer timer = MetricsFactory.getMetrics().getTimer("network.connector.channel.write")) {
            int writtenData = socketChannel.write(data);

            MetricsFactory.getMetrics().getCounter("network.connector.channel.out").inc(writtenData);

            log.debug("Written `{}` bytes to channel: {}", writtenData, socketChannel.getRemoteAddress());


            Verify.verify(Integer.compare(data.limit(), writtenData) == 0);
            return writtenData;
        } catch (IOException e) {
            log.warn("Cant write data to channel {}", socketChannel, e);

            throw new HostNotAvailableException();
        }
    }

    @Override
    protected void finalize() throws Throwable {
        selector.close();
    }
}
