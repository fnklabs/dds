package com.fnklabs.dds.network;

import com.fnklabs.dds.network.connector.exception.ConnectionException;
import com.fnklabs.dds.network.exception.TimeoutException;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.AbstractFuture;

public class ResponseFuture extends AbstractFuture<Message> {
    private static final int TIMEOUT = 15000;

    private long createdAt = System.currentTimeMillis();

    public void onResponse(Message connectorMessage) {
        set(connectorMessage);
    }

    public void onException(HostAndPort address, Throwable throwable) {
        setException(new ConnectionException(throwable.getMessage(), address));
    }

    public void onTimeout(HostAndPort address, long latency, int retryCount) {
        setException(new TimeoutException());
    }

    public boolean isExpired() {
        return getLatency() > TIMEOUT;
    }

    public void onTimeout() {
        onTimeout(null, getLatency(), 1);
    }

    private long getLatency() {
        return System.currentTimeMillis() - createdAt;
    }
}
