package com.fnklabs.dds.network.client;

import com.fnklabs.dds.network.RemoteHostIsNotAvailable;
import com.fnklabs.dds.network.ReplyMessage;
import com.google.common.net.HostAndPort;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.function.Consumer;

@Slf4j
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
    public NetworkClient build(HostAndPort remoteAddress, Consumer<ReplyMessage> messageConsumer) throws ClientException {
        try {
            return new NetworkClient(remoteAddress, messageConsumer);
        } catch (IOException e) {
            log.warn("Can't build client connector", e);
        }

        throw new RemoteHostIsNotAvailable();
    }
}
