package com.fnklabs.dds.network.client.exception;

import com.fnklabs.dds.network.ResponseFuture;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

public class RemoveTimeoutRequest implements Runnable {
    private final ExecutorService executorService;
    private final ConcurrentHashMap<Long, ResponseFuture> pendingRequest;

    public RemoveTimeoutRequest(ExecutorService executorService, ConcurrentHashMap<Long, ResponseFuture> pendingRequest) {
        this.executorService = executorService;
        this.pendingRequest = pendingRequest;
    }

    @Override
    public void run() {
        pendingRequest.forEach((key, value) -> {
            if (value.isExpired()) {
                value.onTimeout();
            }
        });

        executorService.submit(this);
    }
}
