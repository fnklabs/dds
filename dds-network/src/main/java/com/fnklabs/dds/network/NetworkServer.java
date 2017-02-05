package com.fnklabs.dds.network;

import com.google.common.base.Verify;
import com.google.common.net.HostAndPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;

@Service
public class NetworkServer {

    private static final Logger LOGGER = LoggerFactory.getLogger(NetworkServer.class);

    private final AtomicBoolean isRunning = new AtomicBoolean(true);

    private final IncomeMessageHandler incomeMessageHandler;

    /**
     * Local pool executor
     */
    private final ThreadPoolExecutor workerPoolExecutor;


    private final NetworkServerConnector networkServerConnector;

    /**
     * Initialize network server but does not start it
     *
     * @param workerPoolExecutor
     * @param incomeMessageHandler New message handler
     */
    @Autowired
    NetworkServer(
            @Qualifier("network.server.listeningAddress") HostAndPort listenAddress,
            @Qualifier("network.server.pool") ThreadPoolExecutor workerPoolExecutor,
            @Value("${network.server.workers:4}") int workers,
            @Value("${network.server.selectors:1}") int selectors,
            IncomeMessageHandler incomeMessageHandler) throws IOException {


        Queue<Message> msgQueue = new ArrayBlockingQueue<Message>(100_000);

        this.incomeMessageHandler = incomeMessageHandler;
        this.workerPoolExecutor = workerPoolExecutor;
        this.networkServerConnector = new NetworkServerConnector(listenAddress, workerPoolExecutor, selectors, msgQueue);

        Verify.verify(workerPoolExecutor.getMaximumPoolSize() >= workers + selectors);

        for (int i = 0; i < workers; i++) {
            workerPoolExecutor.submit(
                    new NetworkServerWorker(
                            msgQueue,
                            isRunning,
                            incomeMessageHandler,
                            this::onNewReply
                    )
            );
        }
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