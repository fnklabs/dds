package com.fnklabs.dds.network.client;

import com.fnklabs.dds.network.ResponseFuture;
import lombok.RequiredArgsConstructor;
import org.jetbrains.annotations.NotNull;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Task that clean pending request by timeout
 */
@RequiredArgsConstructor
class RemovePendingRequestTask implements Runnable {
    /**
     * Pending request map
     */
    @NotNull
    private final Map<Long, ResponseFuture> pendingRequest;

    @NotNull
    private final AtomicBoolean isRunning;


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
