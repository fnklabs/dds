package com.fnklabs.dds.network;

import com.google.common.net.HostAndPort;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Configuration
@ComponentScan("com.fnklabs.dds.network")
public class NetworkConfiguration {

    @Bean(name = "network.server.pool", destroyMethod = "shutdown")
    public ThreadPoolExecutor selectorPool(@Value("${network.server.pool_size:8}") int poolSize, @Value("${network.selector.queue_size:500}") int queueSize) {
        return new ThreadPoolExecutor(
                poolSize,
                poolSize,
                0L,
                TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(queueSize),
                new com.fnklabs.concurrent.ThreadFactory("network-server-io"),
                new ThreadPoolExecutor.AbortPolicy()
        );

    }

    @Bean(name = "network.server.listeningAddress")
    public HostAndPort listenAddress(@Value("${network.listen_host:127.0.0.1}") String host, @Value("${network.listen_host:10000}") int port) {
        return HostAndPort.fromParts(host, port);
    }
}
