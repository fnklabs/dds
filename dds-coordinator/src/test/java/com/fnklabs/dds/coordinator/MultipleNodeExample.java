package com.fnklabs.dds.coordinator;

import com.google.common.collect.Sets;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import com.google.common.util.concurrent.MoreExecutors;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

import java.util.HashSet;
import java.util.concurrent.ThreadPoolExecutor;

public class MultipleNodeExample {
    public static void main(String[] args) {
        ListeningExecutorService listeningExecutorService = getListeningExecutorService();

        ListeningScheduledExecutorService listeningScheduledExecutorService = getScheduledExecutorService();

        HostAndPort firstNode = HostAndPort.fromString("127.0.0.1:10000");
        HostAndPort secondNode = HostAndPort.fromString("127.0.0.1:10001");
        HashSet<HostAndPort> members = Sets.newHashSet(firstNode, secondNode);

        try {
            TestNodeFactory nodeFactory = new TestNodeFactory(listeningExecutorService, listeningScheduledExecutorService, secondNode);
            Ring ring = new Ring(members, secondNode, nodeFactory, "TestLocal");

            LocalNode node = (LocalNode) nodeFactory.get(secondNode);

            node.joinRing(ring);

        } catch (Throwable e) {
            e.printStackTrace();
        }
    }

    protected static ListeningScheduledExecutorService getScheduledExecutorService() {
        ThreadPoolTaskScheduler threadPoolTaskScheduler = new ThreadPoolTaskScheduler();
        threadPoolTaskScheduler.setPoolSize(1);
        threadPoolTaskScheduler.setThreadNamePrefix("Scheduler-");
        threadPoolTaskScheduler.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        threadPoolTaskScheduler.setWaitForTasksToCompleteOnShutdown(true);
        threadPoolTaskScheduler.setAwaitTerminationSeconds(360);
        threadPoolTaskScheduler.initialize();
        threadPoolTaskScheduler.setBeanName("scheduler");
        threadPoolTaskScheduler.initialize();
        threadPoolTaskScheduler.setErrorHandler(t -> LoggerFactory.getLogger(MultipleNodeExample.class).warn("Execution exception", t));


        return MoreExecutors.listeningDecorator(threadPoolTaskScheduler.getScheduledExecutor());
    }

    protected static ListeningExecutorService getListeningExecutorService() {
        ThreadPoolTaskExecutor threadPoolTaskExecutor = new ThreadPoolTaskExecutor();
        threadPoolTaskExecutor.setCorePoolSize(4);
        threadPoolTaskExecutor.setMaxPoolSize(4);
        threadPoolTaskExecutor.setQueueCapacity(1000);
        threadPoolTaskExecutor.setThreadNamePrefix("ClientWorker-");
        threadPoolTaskExecutor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        threadPoolTaskExecutor.setWaitForTasksToCompleteOnShutdown(true);
        threadPoolTaskExecutor.setAwaitTerminationSeconds(360);
        threadPoolTaskExecutor.initialize();

        return MoreExecutors.listeningDecorator(threadPoolTaskExecutor.getThreadPoolExecutor());
    }
}