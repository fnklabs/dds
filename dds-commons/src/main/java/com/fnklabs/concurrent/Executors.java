package com.fnklabs.concurrent;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public final class Executors {
    private Executors() {
    }

    public static ScheduledExecutorService scheduler(int poolSize, String name) {
        return java.util.concurrent.Executors.newScheduledThreadPool(
                poolSize,
                new com.fnklabs.concurrent.ThreadFactory(name)
        );
    }

    public static ThreadPoolExecutor fixedPoolExecutor(int poolSize, String name) {

        return new ThreadPoolExecutor(
                poolSize,
                poolSize,
                Integer.MAX_VALUE,
                TimeUnit.DAYS,
                new ArrayBlockingQueue<>(500),
                new ThreadFactory(name)
        );
    }
}
