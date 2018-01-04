package com.fnklabs.dds.coordinator;

import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

class WatchDog implements Runnable {
    private static final Logger LOGGER = LoggerFactory.getLogger(WatchDog.class);

    @NotNull
    private final ServerNode serverNode;

    @NotNull
    private final ExecutorService executorService;

    @NotNull
    private final AtomicBoolean isRunning;

    public WatchDog(@NotNull ServerNode serverNode, @NotNull AtomicBoolean isRunning, @NotNull ExecutorService executorService) {
        this.serverNode = serverNode;
        this.isRunning = isRunning;
        this.executorService = executorService;
    }

    @Override
    public void run() {
        if (isRunning.get()) {
            Node.NodeStatus nodeStatus = serverNode.getNodeStatus();

            if (nodeStatus == Node.NodeStatus.START_UP) {
                serverNode.onStartUp();
            } else if (nodeStatus == Node.NodeStatus.SETUP) {
                serverNode.onSetUp();
            } else if (nodeStatus == Node.NodeStatus.SHUTDOWN) {
                serverNode.close();
            } else if (nodeStatus == Node.NodeStatus.REPAIR) {
                serverNode.onRepair();
            }


            executorService.submit(this);
        }

    }
}
