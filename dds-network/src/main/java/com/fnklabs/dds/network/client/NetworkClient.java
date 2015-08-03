package com.fnklabs.dds.network.client;

import com.fnklabs.dds.network.ApiVersion;
import com.fnklabs.dds.network.Message;
import com.fnklabs.dds.network.ResponseFuture;
import com.fnklabs.dds.network.StatusCode;
import com.fnklabs.dds.network.exception.RequestException;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public class NetworkClient implements Closeable {

    /**
     * Is client running
     */
    private final AtomicBoolean isRunning = new AtomicBoolean(true);
    /**
     * Client connector instance
     */
    private final ClientConnector connector;


    /**
     * Response futures map
     */
    private Map<Long, ResponseFuture> responseFutures = new ConcurrentHashMap<>();

    private NetworkClient(ClientConnector connector) {
        this.connector = connector;
    }

    /**
     * Send message
     *
     * @throws RequestException if send request message
     */
    public ResponseFuture send(ByteBuffer data) throws RequestException {
        ResponseFuture responseFuture = new ResponseFuture();

        long messageId = Message.getNextId();

        Message message = new Message(messageId, 0, StatusCode.OK, ApiVersion.VERSION_1, data.array(), 0);

        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(out);
            objectOutputStream.writeObject(message);

            byte[] bytes = out.toByteArray();


            connector.send(ByteBuffer.wrap(bytes));
        } catch (IOException e) {
            e.printStackTrace();
        }


        responseFutures.put(messageId, responseFuture);

        return responseFuture;
    }

    @Override
    public void close() {
        connector.close();
        isRunning.set(false);
    }

    private void join(ExecutorService executorService, Consumer<Message> messageConsumer) {
        connector.join(executorService);

        executorService.submit(new MessageHandlerTask(executorService, connector.getMessagesFromServer(), messageConsumer, responseFutures, isRunning));
        executorService.submit(new RemovePendingRequestTask(responseFutures, executorService, isRunning));
    }

    /**
     * Create client instance
     *
     * @return Client instance
     */
    protected static NetworkClient create(ClientConnector clientConnector, ExecutorService executorService, Consumer<Message> messageConsumer) {
        NetworkClient networkClient = new NetworkClient(clientConnector);
        networkClient.join(executorService, messageConsumer);

        return networkClient;
    }
}
