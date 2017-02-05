package com.fnklabs.dds.network;

import com.fnklabs.concurrent.Executors;
import com.fnklabs.concurrent.ThreadFactory;
import com.google.common.net.HostAndPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

public class NetworkServer implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(NetworkServer.class);

    private final AtomicBoolean isRunning = new AtomicBoolean(true);

    /**
     * Local pool executor
     */
    private final ThreadPoolExecutor workerPoolExecutor;


    private final NetworkServerConnector networkServerConnector;

    /**
     * Initialize network server but does not start it
     *
     * @param incomeMessageHandler New message handler
     */
    NetworkServer(HostAndPort listenAddress, int workers, IncomeMessageHandler incomeMessageHandler) throws IOException {
        Queue<Message> msgQueue = new ArrayBlockingQueue<>(10_000);

        this.workerPoolExecutor = Executors.getThreadPoolExecutor(workers + 1, "network.server.worker");
        this.networkServerConnector = new NetworkServerConnector(Executors.getThreadPoolExecutor(1, "network.server.io"), listenAddress, msgQueue);

        workerPoolExecutor.submit(new NetworkServerWorker(msgQueue, isRunning, incomeMessageHandler, this::onNewReply, workerPoolExecutor));
    }

    @Override
    public void close() throws IOException {
        isRunning.set(false);
        networkServerConnector.close();

        workerPoolExecutor.shutdown();
    }

    /**
     * Send message to client
     */
    protected boolean send(long client, ByteBuffer data) {
        try {
            ByteBuffer msgData = Message.pack(new Message(StatusCode.OK, ApiVersion.CURRENT, data.array()));

            networkServerConnector.sendMessageToClient(client, msgData);
        } catch (Exception e) {
            LOGGER.warn("Can't send data to client", e);

            return false;
        }

        return true;
    }

    private boolean reply(long client, long replyMessageId, byte[] data) {
        try {
            networkServerConnector.sendMessageToClient(client, Message.pack(new Message(replyMessageId, StatusCode.OK, ApiVersion.CURRENT, data)));
        } catch (Exception e) {
            LOGGER.warn("Can't send data to client", e);

            return false;
        }

        return true;
    }

    private void onNewReply(long client, Message message) {
        LOGGER.debug("Receive reply message: {}", message);

        boolean send = reply(client, message.getReplyMessageId(), message.getMessageData());

        LOGGER.debug("Message with id: {} was send with status: {}", message.getId(), send);
    }
}