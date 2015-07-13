package com.fnklabs.dds.network;

import com.fnklabs.dds.network.connector.exception.ConnectionException;
import com.fnklabs.dds.network.connector.MessageBuffer;
import com.fnklabs.dds.network.connector.MessageUtils;
import com.fnklabs.dds.network.connector.NetworkMessage;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.AbstractFuture;

public class ResponseFuture extends AbstractFuture<MessageBuffer> {
    private static final int timeout = 15000;

    private long createdAt = System.currentTimeMillis();

    public void onResponse(MessageBuffer connectorMessageBuffer) {
        set(connectorMessageBuffer);
    }

    public void onException(HostAndPort address, Throwable throwable) {
        setException(new ConnectionException(throwable.getMessage(), address));
    }

    public void onTimeout(HostAndPort address, long latency, int retryCount) {
        NetworkMessage<Boolean> networkMessage = new NetworkMessage<>();
        networkMessage.setStatusCode(StatusCode.TIMEOUT);

        set(MessageUtils.transform(networkMessage));
    }

    public boolean isExpired() {
        return getLatency() > timeout;
    }


    public void onTimeout() {
        onTimeout(null, getLatency(), 1);
    }

    protected long getLatency() {
        return System.currentTimeMillis() - createdAt;
    }
}
