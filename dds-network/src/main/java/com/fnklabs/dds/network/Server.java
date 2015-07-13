package com.fnklabs.dds.network;

import com.fnklabs.dds.network.connector.ServerConnector;
import com.fnklabs.dds.network.exception.ServerException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.io.IOException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicBoolean;

public class Server {

    private static final Logger LOGGER = LoggerFactory.getLogger(Server.class);


    /**
     * Server worker count
     */
    private static final int WORKER_COUNT = 4;
    private static final int EXECUTOR_QUEUE_CAPACITY = 5000;
    private static final int AWAIT_TERMINATION_SECONDS = 10;

    private AtomicBoolean isRunning = new AtomicBoolean(true);

    private ContextHandler contextHandler;

    private ServerConnector serverConnector;

    /**
     * Local pool executor
     */
    private ThreadPoolTaskExecutor workerPoolTaskExecutor;
    private ThreadPoolTaskExecutor serverPoolTaskExecutor;


    private Server(String ip, int port) {
        workerPoolTaskExecutor = new ThreadPoolTaskExecutor();
        workerPoolTaskExecutor.setCorePoolSize(WORKER_COUNT);
        workerPoolTaskExecutor.setMaxPoolSize(WORKER_COUNT);
        workerPoolTaskExecutor.setQueueCapacity(EXECUTOR_QUEUE_CAPACITY);
        workerPoolTaskExecutor.setThreadNamePrefix("ServerWorker-");
        workerPoolTaskExecutor.setRejectedExecutionHandler(new ThreadPoolExecutor.AbortPolicy());
        workerPoolTaskExecutor.setWaitForTasksToCompleteOnShutdown(true);
        workerPoolTaskExecutor.setAwaitTerminationSeconds(AWAIT_TERMINATION_SECONDS);
        workerPoolTaskExecutor.initialize();


        serverPoolTaskExecutor = new ThreadPoolTaskExecutor();
        serverPoolTaskExecutor.setCorePoolSize(2);
        serverPoolTaskExecutor.setMaxPoolSize(2);
        serverPoolTaskExecutor.setQueueCapacity(EXECUTOR_QUEUE_CAPACITY);
        serverPoolTaskExecutor.setThreadNamePrefix("Server-");
        serverPoolTaskExecutor.setRejectedExecutionHandler(new ThreadPoolExecutor.AbortPolicy());
        serverPoolTaskExecutor.setWaitForTasksToCompleteOnShutdown(true);
        serverPoolTaskExecutor.setAwaitTerminationSeconds(AWAIT_TERMINATION_SECONDS);
        serverPoolTaskExecutor.initialize();

        serverConnector = new ServerConnector(ip, port, serverPoolTaskExecutor.getThreadPoolExecutor());
    }

    /**
     * Create server instance
     *
     * @param ip   on which we will bind
     * @param port on which we will bind
     */
    public static Server create(String ip, int port) throws IOException {
        return new Server(ip, port);
    }

    public void setContextHandler(ContextHandler contextHandler) {
        this.contextHandler = contextHandler;
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


        for (int i = 0; i < WORKER_COUNT; i++) {
            ServerWorker task = new ServerWorker(serverConnector.getNewRequestBuffers(), isRunning, contextHandler, connectorMessageBuffer -> {
                serverConnector.sendMessageToClient(connectorMessageBuffer);
            });

            workerPoolTaskExecutor.submit(task);
        }

        serverConnector.create();
        serverConnector.join();
    }

    public void stop() {
        serverConnector.stop();
        isRunning.set(false);

        workerPoolTaskExecutor.shutdown();
        serverPoolTaskExecutor.shutdown();
    }


}