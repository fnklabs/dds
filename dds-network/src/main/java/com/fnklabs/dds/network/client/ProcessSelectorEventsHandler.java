package com.fnklabs.dds.network.client;

import java.util.concurrent.ExecutorService;

/**
 * Runnable task for processing selector events will call method and add self to executor queue for new processing
 */
class ProcessSelectorEventsHandler implements Runnable {
    private ClientConnector clientConnector;

    private ExecutorService executorService;

    public ProcessSelectorEventsHandler(ClientConnector clientConnector, ExecutorService executorService) {
        this.clientConnector = clientConnector;
        this.executorService = executorService;
    }

    @Override
    public void run() {
        clientConnector.processSelectorEvents();

        executorService.submit(this);
    }
}
