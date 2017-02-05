package com.fnklabs.dds.network;

import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.AbstractFuture;
import lombok.RequiredArgsConstructor;
import org.jetbrains.annotations.Nullable;

import java.util.concurrent.TimeUnit;

@RequiredArgsConstructor
public class ResponseFuture extends AbstractFuture<Message> {
    private static final int TIMEOUT = 30_000;
    private final Message message;

    private final long createdAt = System.nanoTime();

    public void onResponse(Message connectorMessage) {
        set(connectorMessage);
    }

    public void onException(HostAndPort address, Throwable throwable) {
        setException(new ConnectionException(throwable.getMessage(), address));
    }

    public void onTimeout(@Nullable HostAndPort address, long latency, int retryCount) {
        setException(new TimeoutException(message));
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
