package com.fnklabs.dds.network.client;

import com.fnklabs.dds.network.ResponseFuture;
import org.jetbrains.annotations.NotNull;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Task that clean pending request by timeout
 */

class RemovePendingRequestTask implements Runnable {
    /**
     * Pending request map
     */

    private final Map<Long, ResponseFuture> pendingRequest;


    private final AtomicBoolean isRunning;

    RemovePendingRequestTask(@NotNull Map<Long, ResponseFuture> pendingRequest, @NotNull AtomicBoolean isRunning) {
        this.pendingRequest = pendingRequest;
        this.isRunning = isRunning;
    }


    @Override
    public void run() {
        if (!isRunning.get()) {
            throw new RuntimeException("Stop execution");
        }

        pendingRequest.forEach((id, future) -> {
            if (future.isExpired()) {
                pendingRequest.remove(id);
                future.onTimeout();
            }
        });
    }
}
