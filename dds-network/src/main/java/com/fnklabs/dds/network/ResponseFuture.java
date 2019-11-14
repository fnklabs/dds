package com.fnklabs.dds.network;

import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.AbstractFuture;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.TimeUnit;

public class ResponseFuture extends AbstractFuture<ReplyMessage> {
    private static final int TIMEOUT = 35_000;
    private final RequestMessage message;

    private final long createdAt = System.nanoTime();

    public ResponseFuture(RequestMessage message) {this.message = message;}

    public void onResponse(ReplyMessage connectorMessage) {
        set(connectorMessage);
    }

    public void onException(HostAndPort address, Throwable throwable) {
        setException(new ConnectionException(throwable.getMessage(), address));
    }

    public void onTimeout(@Nullable HostAndPort address, long latency, int retryCount) {
        setException(new TimeoutException(message, TimeUnit.MILLISECONDS.convert(latency, TimeUnit.NANOSECONDS)));
    }

    public boolean isExpired() {
        return TimeUnit.MILLISECONDS.convert(getLatency(), TimeUnit.NANOSECONDS) > TIMEOUT;
    }

    public void onTimeout() {
        onTimeout(null, getLatency(), 1);
    }

    public long getLatency() {
        return System.nanoTime() - createdAt;
    }
}
