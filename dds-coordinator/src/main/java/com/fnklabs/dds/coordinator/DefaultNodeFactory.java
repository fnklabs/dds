package com.fnklabs.dds.coordinator;

import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
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

public class DefaultNodeFactory implements NodeFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultNodeFactory.class);

    private ConcurrentHashMap<HostAndPort, Node> nodes = new ConcurrentHashMap<>();

    private ListeningExecutorService executorService;

    private ListeningScheduledExecutorService scheduledExecutorService;

    public DefaultNodeFactory(ListeningExecutorService executorService, ListeningScheduledExecutorService scheduledExecutorService) {
        this.executorService = executorService;
        this.scheduledExecutorService = scheduledExecutorService;
    }

    @Override
    public synchronized Node get(HostAndPort hostAndPort) {

        if (nodes.containsKey(hostAndPort)) {
            return nodes.get(hostAndPort);
        }

        Node node = build(hostAndPort);

        if (node != null) {
            nodes.putIfAbsent(hostAndPort, node);
        }

        return node;
    }

    @Override
    public void remove(HostAndPort nodeInfo) {
        Node node = nodes.get(nodeInfo);

        if (node instanceof RemoteNode) {
            ((RemoteNode) node).shutdown();
        }

        nodes.remove(nodeInfo);
    }

    /**
     * Check whether specified address is local
     *
     * @param address Address to check
     *
     * @return true if address is local
     */
    public boolean isLocal(HostAndPort address) {
        try {
            Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();

            while (networkInterfaces.hasMoreElements()) {
                NetworkInterface networkInterface = networkInterfaces.nextElement();
                Enumeration<InetAddress> inetAddresses = networkInterface.getInetAddresses();

                while (inetAddresses.hasMoreElements()) {
                    InetAddress i = inetAddresses.nextElement();

                    LOGGER.debug("Local interface: {}", i.getHostAddress());

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
    private Node build(@NotNull HostAndPort hostAndPort) {

        try {
            if (isLocal(hostAndPort)) {
                return new LocalNode(hostAndPort, executorService, scheduledExecutorService, this);
            } else {
                return new RemoteNode(hostAndPort, executorService);
            }
        } catch (IOException e) {
            LOGGER.warn("Cant build node: " + hostAndPort, e);
        }

        return null;
    }
}
