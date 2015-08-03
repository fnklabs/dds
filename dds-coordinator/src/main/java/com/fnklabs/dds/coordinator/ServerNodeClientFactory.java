package com.fnklabs.dds.coordinator;

import com.google.common.net.HostAndPort;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;

public class ServerNodeClientFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(ServerNodeClientFactory.class);

    private static final ThreadPoolTaskExecutor THREAD_POOL_EXECUTOR;

    private static final int CORE_POOL_SIZE = 2;
    private static final int MAX_POOL_SIZE = 2;
    private static final int QUEUE_CAPACITY = 500;
    private static final int AWAIT_TERMINATION_SECONDS = 60;


    private static final ConcurrentHashMap<HostAndPort, ServerNodeClient> nodes = new ConcurrentHashMap<>();

    protected ServerNodeClientFactory() {
    }

    /**
     * Get node instance
     *
     * @param nodeAddress Node address
     *
     * @return Node instance
     */
    @Nullable
    public synchronized ServerNodeClient getInstance(HostAndPort nodeAddress) {
        ServerNodeClient serverNodeClient = nodes.get(nodeAddress);

        if (serverNodeClient == null) {
            serverNodeClient = build(nodeAddress);
        }

        return serverNodeClient;
    }

    /**
     * Check whether specified address is local
     *
     * @param address Address to check
     *
     * @return true if address is local
     */
    private static boolean isLocal(HostAndPort address) {
        try {
            Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();

            while (networkInterfaces.hasMoreElements()) {
                NetworkInterface networkInterface = networkInterfaces.nextElement();
                Enumeration<InetAddress> inetAddresses = networkInterface.getInetAddresses();

                while (inetAddresses.hasMoreElements()) {
                    InetAddress i = inetAddresses.nextElement();

                    LOGGER.debug("Local interface {} address: {}", networkInterface.getName(), i.getHostAddress());

                    if (StringUtils.equals(i.getHostAddress(), address.getHostText())) {
                        return true;
                    }
                }
            }
        } catch (SocketException e1) {
            LOGGER.warn("Cant get local address", e1);
        }


        return false;
    }

    @Nullable
    private static ServerNodeClient build(@NotNull HostAndPort hostAndPort) {
        //            if (isLocal(hostAndPort)) {
//                return new ServerNode(hostAndPort, THREAD_POOL_EXECUTOR.getThreadPoolExecutor());
//            } else {
        return new ServerNodeClient(hostAndPort, THREAD_POOL_EXECUTOR.getThreadPoolExecutor());

//            }
    }

    static {
        THREAD_POOL_EXECUTOR = new ThreadPoolTaskExecutor();
        THREAD_POOL_EXECUTOR.setCorePoolSize(CORE_POOL_SIZE);
        THREAD_POOL_EXECUTOR.setMaxPoolSize(MAX_POOL_SIZE);
        THREAD_POOL_EXECUTOR.setQueueCapacity(QUEUE_CAPACITY);
        THREAD_POOL_EXECUTOR.setThreadNamePrefix("Coordinator-");
        THREAD_POOL_EXECUTOR.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        THREAD_POOL_EXECUTOR.setWaitForTasksToCompleteOnShutdown(true);
        THREAD_POOL_EXECUTOR.setAwaitTerminationSeconds(AWAIT_TERMINATION_SECONDS);
        THREAD_POOL_EXECUTOR.initialize();
    }
}
