package com.fnklabs.dds.coordinator;

import com.google.common.net.HostAndPort;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

public class ServerNodeClientFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(ServerNodeClientFactory.class);

    private static final ThreadPoolExecutor CLIENT_THREAD_POOL_EXECUTOR;

    private static final int CORE_POOL_SIZE = 2;
    private static final int MAX_POOL_SIZE = 2;
    private static final int QUEUE_CAPACITY = 500;

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
    public ServerNodeClient getInstance(HostAndPort nodeAddress) {
        return nodes.computeIfAbsent(nodeAddress, new Function<HostAndPort, ServerNodeClient>() {
            @Override
            public ServerNodeClient apply(HostAndPort hostAndPort) {
                return build(nodeAddress);
            }
        });
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
        return new ServerNodeClient(hostAndPort, CLIENT_THREAD_POOL_EXECUTOR);
    }

    static {

        CLIENT_THREAD_POOL_EXECUTOR = new ThreadPoolExecutor(
                CORE_POOL_SIZE,
                MAX_POOL_SIZE,
                0L,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<Runnable>(QUEUE_CAPACITY),
                new com.fnklabs.concurrent.ThreadFactory("network-worker-"),
                new ThreadPoolExecutor.CallerRunsPolicy()
        );

    }
}
