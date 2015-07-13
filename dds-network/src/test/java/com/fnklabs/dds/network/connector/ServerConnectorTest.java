package com.fnklabs.dds.network.connector;

import com.fnklabs.dds.Metrics;
import com.fnklabs.dds.network.Operation;
import com.fnklabs.dds.network.ResponseFuture;
import com.fnklabs.dds.network.client.Client;
import com.fnklabs.dds.network.client.ClientFactory;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.MoreExecutors;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

public class ServerConnectorTest {
    @After
    public void tearDown() throws Exception {
        Metrics.reporter.report();
    }

    @Test
    public void testGetNewRequestBuffers() throws Exception {
        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(2, 2, 100, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(100));
        ServerConnector serverConnector = new ServerConnector("127.0.0.1", 10000, threadPoolExecutor);
        serverConnector.create();
        serverConnector.join();

        ArrayBlockingQueue<MessageBuffer> newRequestBuffers = serverConnector.getNewRequestBuffers();

        Assert.assertNotNull(newRequestBuffers);

        Assert.assertEquals(0, newRequestBuffers.size());

        Client client = ClientFactory.build(HostAndPort.fromString("127.0.0.1:10000"));


        ResponseFuture send = client.send(new Operation() {
            @Override
            public int getCode() {
                return 123;
            }
        }, Boolean.FALSE);

        try {
            send.get(1, TimeUnit.SECONDS);
        } catch (TimeoutException e) {

        }

        Assert.assertEquals(1, newRequestBuffers.size());

        MessageBuffer poll = newRequestBuffers.poll();

        Message<Boolean> transform = MessageUtils.<Boolean>transform(poll);
        Assert.assertNotNull(transform);
        Assert.assertEquals(123, transform.getOperationCode());
        Assert.assertFalse(transform.getData());

        serverConnector.stop();
        client.close();
    }

    @Test
    public void testJoin() throws Exception {
        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(2, 2, 100, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(100));
        ServerConnector serverConnector = new ServerConnector("127.0.0.1", 10000, threadPoolExecutor);
        serverConnector.create();
        serverConnector.join();

        serverConnector.stop();
    }

    @Test
    public void testSendMessageToClient() throws Exception {
        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(2, 2, 100, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(100));
        ServerConnector serverConnector = new ServerConnector("127.0.0.1", 10000, threadPoolExecutor);
        serverConnector.create();
        serverConnector.join();

        ArrayBlockingQueue<MessageBuffer> newRequestBuffers = serverConnector.getNewRequestBuffers();

        Assert.assertNotNull(newRequestBuffers);

        Assert.assertEquals(0, newRequestBuffers.size());

        AtomicBoolean clientRetrievedMessage = new AtomicBoolean(false);

        Client client = ClientFactory.build(HostAndPort.fromString("127.0.0.1:10000"));

        ResponseFuture send = client.send(new Operation() {
            @Override
            public int getCode() {
                return 123;
            }
        }, Boolean.FALSE);

        try {
            send.get(1, TimeUnit.SECONDS);
        } catch (TimeoutException e) {

        }

        Assert.assertEquals(1, newRequestBuffers.size());

        MessageBuffer poll = newRequestBuffers.poll();

        Message<Boolean> transform = MessageUtils.<Boolean>transform(poll);
        Assert.assertNotNull(transform);
        Assert.assertEquals(123, transform.getOperationCode());
        Assert.assertFalse(transform.getData());

        NetworkMessage<Boolean> networkMessage = new NetworkMessage<>();
        networkMessage.setClientId(1);
        networkMessage.setId(123);
        networkMessage.setReplyMessageId(123);
        networkMessage.setData(Boolean.FALSE);
        networkMessage.setOperationCode(123);

        serverConnector.sendMessageToClient(MessageUtils.transform(networkMessage));
        Thread.sleep(1000);

//        Assert.assertTrue(clientRetrievedMessage.get());

        for (int i = 0; i < 500; i++) {
            client.send(new Operation() {
                @Override
                public int getCode() {
                    return 0;
                }
            }, new Long("1"));
        }

        serverConnector.stop();
    }

    @Test
    public void testCreate() throws Exception {
        ServerConnector serverConnector = new ServerConnector("127.0.0.1", 10000, MoreExecutors.newDirectExecutorService());
        serverConnector.create();
    }

    @Test
    public void testStop() throws Exception {
        ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(2, 2, 100, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(100));
        ServerConnector serverConnector = new ServerConnector("127.0.0.1", 10000, threadPoolExecutor);
        serverConnector.create();
        serverConnector.join();

        serverConnector.stop();
    }
}