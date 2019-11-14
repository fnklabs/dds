package com.fnklabs.dds.network.server;

import com.fnklabs.dds.network.ReplyMessage;
import com.fnklabs.dds.network.ResponseFuture;
import com.fnklabs.dds.network.client.NetworkClient;
import com.fnklabs.dds.network.client.NetworkClientFactory;
import com.fnklabs.dds.network.pool.NetworkExecutor;
import com.fnklabs.dds.network.pool.NioExecutor;
import com.fnklabs.metrics.MetricsFactory;
import com.fnklabs.metrics.Timer;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.Futures;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.stream.IntStream;


public class NetworkServerTest {
    private final static Logger log = LoggerFactory.getLogger(NetworkServerTest.class);

    private NetworkServer networkServer;

    private NetworkExecutor serverExecutor;

    private NetworkExecutor clientExecutor;

    private NetworkClientFactory networkClientFactory;

    private HostAndPort hostAndPort = HostAndPort.fromParts("127.0.0.1", 10_000);

    @Before
    public void setUp() throws Exception {
        serverExecutor = NioExecutor.builder().build();
        serverExecutor.run();

        clientExecutor = NioExecutor.builder().build();
        clientExecutor.run();

        networkServer = new NetworkServer(hostAndPort, 4, new TestIncomeMessageHandler());
        networkServer.join(serverExecutor);

        networkClientFactory = new NetworkClientFactory();
    }

    @After
    public void tearDown() throws Exception {
        networkServer.close();
        serverExecutor.shutdown();

        MetricsFactory.getMetrics().report();
    }

    @Test
    public void sync() throws Exception {
        NetworkClient client = networkClientFactory.build(hostAndPort, message -> log.debug("New message from server: {}", message));
        client.join(clientExecutor);

        ByteBuffer dataBuffer = ByteBuffer.allocate(Integer.BYTES);

        Timer totalTimer = MetricsFactory.getMetrics().getTimer("total.time");

        for (int i = 0; i < 100_000; i++) {
            Timer timer = MetricsFactory.getMetrics().getTimer("test.message.send");

            dataBuffer.putInt(0, i);

            ResponseFuture future = client.send(dataBuffer);

            log.debug("Awaiting message: {}", i);

            ReplyMessage replyMessage = future.get();

            timer.stop();

            ByteBuffer data = ByteBuffer.wrap(replyMessage.getData());

            Assert.assertEquals(i, data.getInt());

            log.debug("Send message: `{}` in {}", i, timer);

        }
        totalTimer.stop();
    }

    @Test
    public void async() throws Exception {
        NetworkClient client = networkClientFactory.build(hostAndPort, message -> log.debug("New message from server: {}", message));
        client.join(clientExecutor);

        IntStream range = IntStream.range(0, 100_000);

        Timer totalTimer = MetricsFactory.getMetrics().getTimer("total.time");
        range.parallel()
             .forEach(message -> {
                 Timer timer = MetricsFactory.getMetrics().getTimer("test.message.send");

                 ByteBuffer dataBuffer = ByteBuffer.allocate(Integer.BYTES);
                 dataBuffer.putInt(0, message);

                 ResponseFuture future = client.send(dataBuffer);

                 log.debug("Awaiting message: {}", message);

                 ReplyMessage replyMessage = Futures.getUnchecked(future);

                 timer.stop();

                 ByteBuffer data = ByteBuffer.wrap(replyMessage.getData());

                 Assert.assertEquals(message, data.getInt());

                 log.debug("Send message: `{}` in {}", message, timer);
             });


        totalTimer.stop();
    }


}
