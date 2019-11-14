package com.fnklabs.dds.network.client;

import com.fnklabs.dds.network.RemoteHostIsNotAvailable;
import com.fnklabs.dds.network.ReplyMessage;
import com.google.common.net.HostAndPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.function.Consumer;


public class NetworkClientFactory {
    private final static Logger log = LoggerFactory.getLogger(NetworkClientFactory.class);

    /**
     * Build new network client
     *
     * @param remoteAddress   Remote host address
     * @param messageConsumer System messages consumer
     *
     * @return New network client
     *
     * @throws ClientException if can't connect to remote host
     */
    public NetworkClient build(HostAndPort remoteAddress, Consumer<ReplyMessage> messageConsumer) throws ClientException {
        try {
            return new NetworkClient(remoteAddress, 1, messageConsumer);
        } catch (IOException e) {
            log.warn("Can't build client connector", e);
        }

        throw new RemoteHostIsNotAvailable();
    }
}
