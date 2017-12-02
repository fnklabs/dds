package com.fnklabs.dds.network;

import com.fnklabs.metrics.Metrics;
import com.fnklabs.metrics.MetricsFactory;
import com.fnklabs.metrics.Timer;
import com.google.common.base.Verify;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.channels.spi.AbstractSelectableChannel;
import java.util.Collections;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;

@Slf4j
public abstract class NetworkConnector<T extends Message> {
    private static final Metrics METRICS = MetricsFactory.getMetrics();
    private final Selector selector = Selector.open();

    private final Queue<T> messageQueue;

    private final ExecutorService executorService;

    public NetworkConnector(ExecutorService executorService, Queue<T> messageQueue) throws IOException {
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
    protected SelectionKey register(AbstractSelectableChannel channel, int operation) throws ClosedChannelException {
        return channel.register(selector, operation);
    }

    protected abstract void readMessagesFromBuffer(ByteBuffer messageBuffer, Consumer<T> newMessageHandler);


    public static void closeChannel(SelectionKey key, SelectableChannel clientSocketChannel) {
        try {
            key.cancel();
            clientSocketChannel.close();
        } catch (IOException e) {
            log.warn("Can't close channel", e);
        }
    }

    protected Set<SelectionKey> selectNewEvents() {
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

    public static int readFromChannel(SocketChannel socketChannel, ByteBuffer buffer) throws ChannelClosedException {
        try (Timer timer = METRICS.getTimer("network.connector.channel.write")) {
            int receivedBytes = socketChannel.read(buffer);

            METRICS.getCounter("network.connector.channel.in").inc(receivedBytes);

            log.debug("received `{}` bytes: from {}", receivedBytes, socketChannel.getRemoteAddress());

            /* Check if socket was closed */
            if (receivedBytes == -1) {
                throw new ChannelClosedException(socketChannel);
            }

            return receivedBytes;
        } catch (IOException e) {
            log.error("Can't write from channel {}/{}", socketChannel, e);
        }

        return 0;
    }


    /**
     * Write data to channel
     *
     * @param data Data/message
     *
     * @throws HostNotAvailableException if cant send data
     */
    public static int write(SocketChannel socketChannel, ByteBuffer data) throws HostNotAvailableException {

        try (Timer timer = METRICS.getTimer("network.connector.channel.write")) {
            int writtenData = socketChannel.write(data);

            METRICS.getCounter("network.connector.channel.out").inc(writtenData);

            log.debug("Written `{}` bytes to channel: {}", writtenData, socketChannel.getRemoteAddress());

            Verify.verify(Integer.compare(data.limit(), writtenData) == 0, "Not all data was written to channel %s of %s", writtenData, data.limit());

            return writtenData;
        } catch (IOException e) {
            log.warn("Cant read data to channel {}", socketChannel, e);

            throw new HostNotAvailableException();
        }
    }

    @Override
    protected void finalize() throws Throwable {
        selector.close();
    }
}
