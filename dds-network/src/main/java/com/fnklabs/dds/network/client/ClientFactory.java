package com.fnklabs.dds.network.client;

import com.fnklabs.dds.network.client.exception.ClientException;
import com.fnklabs.dds.network.client.exception.RemoteHostIsNotAvailable;
import com.google.common.net.HostAndPort;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.io.IOException;
import java.util.concurrent.ThreadPoolExecutor;

public class ClientFactory {
    private static final int CORE_POOL_SIZE = 2;
    private static final int MAX_POOL_SIZE = 2;
    private static final ThreadPoolTaskExecutor THREAD_POOL_EXECUTOR;
    private static final int QUEUE_CAPACITY = 500;

    private static final int AWAIT_TERMINATION_SECONDS = 360;

    public static Client build(HostAndPort remoteAddress) throws ClientException {
        try {
            ClientConnector clientConnector = ClientConnector.build(remoteAddress);
            return Client.create(clientConnector, getThreadPoolExecutor());
        } catch (IOException e) {
            LoggerFactory.getLogger(ClientFactory.class).warn("Cant build client connector", e);
        }

        throw new RemoteHostIsNotAvailable();
    }

    private static ThreadPoolExecutor getThreadPoolExecutor() {
        return THREAD_POOL_EXECUTOR.getThreadPoolExecutor();
    }

    static {
        THREAD_POOL_EXECUTOR = new ThreadPoolTaskExecutor();
        THREAD_POOL_EXECUTOR.setCorePoolSize(CORE_POOL_SIZE);
        THREAD_POOL_EXECUTOR.setMaxPoolSize(MAX_POOL_SIZE);
        THREAD_POOL_EXECUTOR.setQueueCapacity(QUEUE_CAPACITY);
        THREAD_POOL_EXECUTOR.setThreadNamePrefix("Client-");
        THREAD_POOL_EXECUTOR.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        THREAD_POOL_EXECUTOR.setWaitForTasksToCompleteOnShutdown(true);
        THREAD_POOL_EXECUTOR.setAwaitTerminationSeconds(AWAIT_TERMINATION_SECONDS);
        THREAD_POOL_EXECUTOR.initialize();
    }
}
