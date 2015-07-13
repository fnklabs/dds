package com.fnklabs.dds.network.client;

import com.fnklabs.dds.network.Operation;
import com.fnklabs.dds.network.exception.RequestException;
import com.fnklabs.dds.network.ResponseFuture;
import com.fnklabs.dds.network.connector.MessageBuffer;

import java.io.Serializable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public class Client {

    /**
     * Is client running
     */
    private final AtomicBoolean isActive = new AtomicBoolean(true);

    /**
     * Client connector instance
     */
    private final ClientConnector clientConnector;

    private Client(ClientConnector clientConnector) {
        this.clientConnector = clientConnector;
    }

    /**
     * Create client instance
     *
     * @return Client instance
     */
    public static Client create(ClientConnector clientConnector, ExecutorService executorService) {
        Client client = new Client(clientConnector);
        client.join(executorService);

        return client;
    }

    /**
     * Send message
     *
     * @param operation Operation type
     *
     * @return Request id
     *
     * @throws RequestException if send request message
     */
    public <T extends Serializable> ResponseFuture send(Operation operation, T data) throws RequestException {
        return clientConnector.send(operation, data);
    }

    public void close() {
        clientConnector.close();
        isActive.set(false);
    }

    protected void join(ExecutorService executorService) {
        executorService.submit(() -> new MessageHandler(executorService, clientConnector.getNotificationMessages(), new Consumer<MessageBuffer>() {
            @Override
            public void accept(MessageBuffer messageBuffer) {
                if (messageBuffer != null) {
                    long replyMessageId = messageBuffer.getReplyMessageId();


                    // todo process message
                }
            }
        }));

        clientConnector.join(executorService);
    }
}
