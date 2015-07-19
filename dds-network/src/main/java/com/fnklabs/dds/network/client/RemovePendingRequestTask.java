package com.fnklabs.dds.network.client;

import com.fnklabs.dds.network.ResponseFuture;
import org.jetbrains.annotations.NotNull;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

class RemovePendingRequestTask implements Runnable {
    @NotNull
    private final Map<Long, ResponseFuture> pendingRequest;

    @NotNull
    private final ExecutorService executorService;

    @NotNull
    private final AtomicBoolean isRunning;

    public RemovePendingRequestTask(@NotNull Map<Long, ResponseFuture> pendingRequest, @NotNull ExecutorService executorService, @NotNull AtomicBoolean isRunning) {
        this.pendingRequest = pendingRequest;
        this.executorService = executorService;
        this.isRunning = isRunning;
    }

    @Override
    public void run() {
        if (isRunning.get()) {
            pendingRequest.forEach((id, future) -> {
                if (future.isExpired()) {
                    future.onTimeout();

                    pendingRequest.remove(id);
                }
            });

            executorService.submit(() -> this);
        }
    }
}
