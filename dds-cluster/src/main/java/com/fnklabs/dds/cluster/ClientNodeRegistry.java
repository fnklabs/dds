package com.fnklabs.dds.cluster;

import com.fnklabs.dds.network.pool.NetworkExecutor;
import com.fnklabs.dds.network.pool.NioExecutor;
import com.google.common.net.HostAndPort;
import org.apache.commons.lang3.StringUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.Enumeration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

class ClientNodeRegistry {
    private static final Logger LOGGER = LoggerFactory.getLogger(ClientNodeRegistry.class);

    private final int poolSize;

    private static final ConcurrentHashMap<HostAndPort, Node> nodes = new ConcurrentHashMap<>();

    private final NetworkExecutor networkExecutor;

    private final Serializer serializer;

    ClientNodeRegistry(int poolSize, Serializer serializer) throws IOException {
        this.poolSize = poolSize;

        networkExecutor = NioExecutor.builder()
                                     .setOpReadExecutor(poolSize)
                                     .setOpWriteExecutor(poolSize)
                                     .build();
        this.serializer = serializer;
    }

    /**
     * Get node instance
     *
     * @param nodeAddress Node address
     *
     * @return Node instance
     */
    @Nullable
    public Node getInstance(HostAndPort nodeAddress) {
        return nodes.computeIfAbsent(nodeAddress, new Function<HostAndPort, Node>() {
            @Override
            public Node apply(HostAndPort hostAndPort) {
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

    private Node build(@NotNull HostAndPort hostAndPort) {
        return new NodeProxy(hostAndPort, serializer);
    }


}
