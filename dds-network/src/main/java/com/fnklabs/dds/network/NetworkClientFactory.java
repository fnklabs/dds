package com.fnklabs.dds.network;

import com.google.common.net.HostAndPort;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.function.Consumer;

@Service
public class NetworkClientFactory {

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
    public NetworkClient build(HostAndPort remoteAddress, Consumer<Message> messageConsumer) throws ClientException {
        try {
            ;
            return new NetworkClient(remoteAddress, messageConsumer);
        } catch (IOException e) {
            LoggerFactory.getLogger(NetworkClientFactory.class).warn("Can't build client connector", e);
        }

        throw new RemoteHostIsNotAvailable();
    }
}
