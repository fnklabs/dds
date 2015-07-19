package com.fnklabs.dds.network.client;

import org.jetbrains.annotations.NotNull;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Runnable task for processing selector events will call method and add self to executor queue for new processing
 */
class ProcessSelectorEventsTask implements Runnable {
    @NotNull
    private final ClientConnector clientConnector;

    @NotNull
    private final ExecutorService executorService;

    @NotNull
    private final AtomicBoolean isRunning;

    public ProcessSelectorEventsTask(@NotNull ClientConnector clientConnector, @NotNull ExecutorService executorService, @NotNull AtomicBoolean isRunning) {
        this.clientConnector = clientConnector;
        this.executorService = executorService;
        this.isRunning = isRunning;
    }

    @Override
    public void run() {
        if (isRunning.get()) {
            clientConnector.processSelectorEvents();

            executorService.submit(this);
        }
    }
}
