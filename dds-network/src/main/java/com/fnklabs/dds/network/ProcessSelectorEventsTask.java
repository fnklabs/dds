package com.fnklabs.dds.network;

import lombok.RequiredArgsConstructor;
import org.jetbrains.annotations.NotNull;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Runnable task for processing selector events will call method and add self to executor queue for new processing
 */
@RequiredArgsConstructor
class ProcessSelectorEventsTask implements Runnable {

    private final NetworkClientConnector networkClientConnector;


    private final ExecutorService executorService;


    private final AtomicBoolean isRunning;

    @Override
    public void run() {

    }
}
