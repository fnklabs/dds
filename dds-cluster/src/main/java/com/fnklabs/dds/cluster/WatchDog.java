package com.fnklabs.dds.cluster;

import java.util.concurrent.atomic.AtomicBoolean;

class WatchDog implements Runnable {
    private final ServerNodeImpl serverNode;

    private final AtomicBoolean isRunning;

    public WatchDog(ServerNodeImpl serverNode, AtomicBoolean isRunning) {
        this.serverNode = serverNode;
        this.isRunning = isRunning;
    }

    @Override
    public void run() {

    }
}
