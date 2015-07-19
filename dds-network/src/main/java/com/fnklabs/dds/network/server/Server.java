package com.fnklabs.dds.network.server;

import com.fnklabs.dds.network.Message;
import com.fnklabs.dds.network.exception.ServerException;
import com.google.common.net.HostAndPort;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

public class Server implements Closeable {

    public static final int SERVER_CORE_POOL_SIZE = 2;
    public static final int SERVER_MAX_POOL_SIZE = 2;
    public static final int WORKER_MAX_POOL_SIZE = 2;
    public static final int WORKER_CORE_POOL_SIZE = 2;

    /**
     * Server worker count
     */
    private static final int WORKER_COUNT = 4;
    private static final int EXECUTOR_QUEUE_CAPACITY = 5000;
    private static final int AWAIT_TERMINATION_SECONDS = 10;

    private final AtomicBoolean isRunning = new AtomicBoolean(true);

    private final MessageHandler messageHandler;

    private ServerConnector serverConnector;

    /**
     * Local pool executor
     */
    private ThreadPoolTaskExecutor workerPoolTaskExecutor;
    private ThreadPoolTaskExecutor serverPoolTaskExecutor;


    private Server(HostAndPort listenInterfaceAndPort, MessageHandler messageHandler) {
        workerPoolTaskExecutor = new ThreadPoolTaskExecutor();
        workerPoolTaskExecutor.setCorePoolSize(WORKER_CORE_POOL_SIZE);
        workerPoolTaskExecutor.setMaxPoolSize(WORKER_MAX_POOL_SIZE);
        workerPoolTaskExecutor.setQueueCapacity(EXECUTOR_QUEUE_CAPACITY);
        workerPoolTaskExecutor.setThreadNamePrefix("ServerWorker-");
        workerPoolTaskExecutor.setRejectedExecutionHandler(new ThreadPoolExecutor.AbortPolicy());
        workerPoolTaskExecutor.setWaitForTasksToCompleteOnShutdown(true);
        workerPoolTaskExecutor.setAwaitTerminationSeconds(AWAIT_TERMINATION_SECONDS);
        workerPoolTaskExecutor.initialize();


        serverPoolTaskExecutor = new ThreadPoolTaskExecutor();
        serverPoolTaskExecutor.setCorePoolSize(SERVER_CORE_POOL_SIZE);
        serverPoolTaskExecutor.setMaxPoolSize(SERVER_MAX_POOL_SIZE);
        serverPoolTaskExecutor.setQueueCapacity(EXECUTOR_QUEUE_CAPACITY);
        serverPoolTaskExecutor.setThreadNamePrefix("Server-");
        serverPoolTaskExecutor.setRejectedExecutionHandler(new ThreadPoolExecutor.AbortPolicy());
        serverPoolTaskExecutor.setWaitForTasksToCompleteOnShutdown(true);
        serverPoolTaskExecutor.setAwaitTerminationSeconds(AWAIT_TERMINATION_SECONDS);
        serverPoolTaskExecutor.initialize();

        serverConnector = new ServerConnector(listenInterfaceAndPort);

        this.messageHandler = messageHandler;
    }

    /**
     * Create server instance
     *
     * @param listenInterfaceAndPort on which we will bind
     */
    public static Server create(HostAndPort listenInterfaceAndPort, MessageHandler messageConsumer) throws IOException {
        return new Server(listenInterfaceAndPort, messageConsumer);
    }

    /**
     * Start server worker and event loop
     *
     * @throws IOException
     */
    public void start() throws IOException, ServerException {
        if (serverConnector == null) {
            throw new ServerException("Connector was not set");
        }

        Consumer<Message> consumer = message -> {
            LoggerFactory.getLogger(Server.class).debug("Receive reply message {}", message.getId());

            boolean send = send(message);

            LoggerFactory.getLogger(Server.class).debug("Message with id: {} was send with status: {}", message.getId(), send);
        };

        for (int i = 0; i < WORKER_COUNT; i++) {
            workerPoolTaskExecutor.submit(new ServerWorker(serverConnector.getNewRequestMessages(), isRunning, messageHandler, workerPoolTaskExecutor.getThreadPoolExecutor(), consumer));
        }

        serverConnector.create();
        serverConnector.join(serverPoolTaskExecutor.getThreadPoolExecutor());
    }

    @Override
    public void close() {
        serverConnector.close();
        isRunning.set(false);

        workerPoolTaskExecutor.shutdown();
        serverPoolTaskExecutor.shutdown();
    }

    /**
     * Send message to client
     *
     * @param message Message
     */
    protected boolean send(Message message) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(out);

            objectOutputStream.writeObject(message);

            ByteBuffer msg = ByteBuffer.wrap(out.toByteArray());
            serverConnector.sendMessageToClient(message.getClient(), msg);
        } catch (Exception e) {
            LoggerFactory.getLogger(Server.class).warn("Can't send data to client", e);

            return false;
        }

        return true;
    }
}