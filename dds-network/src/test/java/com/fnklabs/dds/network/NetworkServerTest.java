package com.fnklabs.dds.network;

import com.fnklabs.metrics.MetricsFactory;
import com.fnklabs.metrics.Timer;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.Futures;
import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.stream.IntStream;

@Slf4j
public class NetworkServerTest {
    private NetworkServer networkServer;

    private NetworkClientFactory networkClientFactory;

    private HostAndPort hostAndPort = HostAndPort.fromParts("127.0.0.1", 10_000);

    @Before
    public void setUp() throws Exception {
        networkServer = new NetworkServer(hostAndPort, 4, new TestIncomeMessageHandler());
        networkClientFactory = new NetworkClientFactory();
    }

    @After
    public void tearDown() throws Exception {
        networkServer.close();
        MetricsFactory.getMetrics().report();
    }

    @Test
    public void sync() throws Exception {
        NetworkClient client = networkClientFactory.build(hostAndPort, message -> log.debug("New message from server: {}", message));

        ByteBuffer dataBuffer = ByteBuffer.allocate(Integer.BYTES);

        Timer totalTimer = MetricsFactory.getMetrics().getTimer("total.time");

        for (int i = 0; i < 100_000; i++) {
            Timer timer = MetricsFactory.getMetrics().getTimer("test.message.send");

            dataBuffer.putInt(0, i);

            ResponseFuture future = client.send(dataBuffer);

            log.debug("Awaiting message: {}", i);

            future.get();

            timer.stop();

            log.debug("Send message: `{}` in {}", i, timer);

        }
        totalTimer.stop();
    }

    @Test
    public void async() throws Exception {
        NetworkClient client = networkClientFactory.build(hostAndPort, message -> log.debug("New message from server: {}", message));

        IntStream range = IntStream.range(0, 100_000);

        Timer totalTimer = MetricsFactory.getMetrics().getTimer("total.time");
        range.parallel()
             .forEach(message -> {
                 Timer timer = MetricsFactory.getMetrics().getTimer("test.message.send");

                 ByteBuffer dataBuffer = ByteBuffer.allocate(Integer.BYTES);
                 dataBuffer.putInt(0, message);

                 ResponseFuture future = client.send(dataBuffer);

                 log.debug("Awaiting message: {}", message);

                 Futures.getUnchecked(future);

                 timer.stop();

                 log.debug("Send message: `{}` in {}", message, timer);
             });


        totalTimer.stop();
    }


}
