package com.fnklabs.dds.network.server;

import org.jetbrains.annotations.NotNull;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

class SelectorProcessingTask implements Runnable {

    @NotNull
    private final ExecutorService executorService;

    @NotNull
    private final ServerConnector serverConnector;

    @NotNull
    private final AtomicBoolean isRunning;

    public SelectorProcessingTask(@NotNull ExecutorService executorService, @NotNull ServerConnector serverConnector, @NotNull AtomicBoolean isRunning) {
        this.executorService = executorService;
        this.serverConnector = serverConnector;
        this.isRunning = isRunning;
    }

    @Override
    public void run() {
        if (isRunning.get()) {
            serverConnector.processSelectorEvents();

            executorService.submit(this);
        }
    }
}
