package com.fnklabs.dds.coordinator;

import com.fnklabs.dds.Metrics;
import com.fnklabs.dds.network.ApiVersion;
import com.fnklabs.dds.network.Message;
import com.fnklabs.dds.network.ResponseFuture;
import com.fnklabs.dds.network.StatusCode;
import com.fnklabs.dds.network.client.Client;
import com.fnklabs.dds.network.client.ClientFactory;
import com.fnklabs.dds.network.exception.ServerException;
import com.fnklabs.dds.network.server.MessageHandler;
import com.fnklabs.dds.network.server.Server;
import com.google.common.net.HostAndPort;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.net.BindException;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

public class ServerTest {
    @Test
    public void testCreate() throws Exception {
        Server server = Server.create(HostAndPort.fromString("127.0.0.1:10000"), new MessageHandler() {
            @Override
            public Message handle(Message message) {
                LoggerFactory.getLogger(getClass()).debug("Retrieving new message from {} id {}", message.getClient(), message.getId());

                return new Message(Message.getNextId(), message.getId(), StatusCode.OK, ApiVersion.VERSION_1, new byte[0], message.getClient());
            }
        });

        Assert.assertNotNull(server);
    }

    @Test
    public void testJoin() throws Exception, ServerException {
        Server server = Server.create(HostAndPort.fromString("127.0.0.1:10000"), new MessageHandler() {
            @Override
            public Message handle(Message message) {
                LoggerFactory.getLogger(getClass()).debug("Retrieving new message from {} id {}", message.getClient(), message.getId());

                return new Message(Message.getNextId(), message.getId(), StatusCode.OK, ApiVersion.VERSION_1, new byte[0], message.getClient());
            }
        });

        server.start();

        server.close();
    }

    @Test(expected = BindException.class)
    public void testJoinWithInvalidInterface() throws Exception, ServerException {
        Server server = Server.create(HostAndPort.fromString("192.168.1.1:10000"), new MessageHandler() {
            @Override
            public Message handle(Message message) {
                LoggerFactory.getLogger(getClass()).debug("Retrieving new message from {} id {}", message.getClient(), message.getId());

                return new Message(Message.getNextId(), message.getId(), StatusCode.OK, ApiVersion.VERSION_1, new byte[0], message.getClient());
            }
        });

        server.start();

        server.close();
    }

    @Test
    public void testName() throws Exception {
//        int i = 0xFF & Byte.MAX_VALUE;
//        int i1 = 0xFF & Byte.MIN_VALUE;

        int i = Byte.toUnsignedInt(Byte.MAX_VALUE);
        int i1 = Byte.toUnsignedInt(Byte.parseByte("-1"));


        LoggerFactory.getLogger(getClass()).debug("Max byte: {} Min byte: {}, {}", i, i1, 0x0);
    }

    @Test
    public void testMessage() throws Exception, ServerException {
        Server server = Server.create(HostAndPort.fromString("127.0.0.1:10000"), new MessageHandler() {
            @Override
            public Message handle(Message message) {
                LoggerFactory.getLogger(getClass()).debug("Retrieving new message from {} id {}", message.getClient(), message.getId());

                return new Message(Message.getNextId(), message.getId(), StatusCode.OK, ApiVersion.VERSION_1, new byte[0], message.getClient());
            }
        });

        server.start();


        Client client = ClientFactory.build(HostAndPort.fromString("127.0.0.1:10000"), new Consumer<Message>() {
            @Override
            public void accept(Message message) {
                LoggerFactory.getLogger(getClass()).debug("Received msg: {}:{}", message.getId(), message.getReplyMessageId());
            }
        });


        for (int i = 0; i < 1000; i++) {
            ResponseFuture responseFuture = client.send(ByteBuffer.wrap(UUID.randomUUID().toString().getBytes()));
            Message message = responseFuture.get(15, TimeUnit.SECONDS);
            Assert.assertNotNull(message);
        }

        server.close();
        client.close();
    }

    //    @Test
//    public void testGive5() throws Exception {
//        HostAndPort hostAndPort = HostAndPort.fromHost("127.0.0.1");
//        NodeInfo nodeInfo = new NodeInfo(hostAndPort, "1");
//
//        Ring ring = mock(Ring.class);
//
//        LocalNode localNode = mock(LocalNode.class);
//        when(localNode.getNodeInfo()).thenReturn(nodeInfo);
//        when(localNode.getRing()).thenReturn(ring);
//
//        when(ring.getRingInfo()).thenReturn(new RingInfo(nodeInfo, Ring.sort(Sets.newHashSet()), nodeInfo));
//
//        NodeFactory nodeFactory = mock(NodeFactory.class);
//        when(nodeFactory.get(any(HostAndPort.class))).thenReturn(localNode);
//
//        ServerEventsHandler serverEventsHandler = new ServerEventsHandler(localNode);
//
//        RequestBuffer requestBuffer = mock(RequestBuffer.class);
//        when(requestBuffer.getOperationCode()).thenReturn(OperationType.CLUSTER_INFO.getCode());
//
//
////        ByteBuffer responseBuffer = serverEventsHandler.handle(requestBuffer);
////
//        verify(requestBuffer).getOperationCode();
//        verify(ring).getRingInfo();
//
//
////        ResponseBuffer response = new ResponseBuffer(client, responseBuffer);
//
////        long id = response.getId();
////        RingInfo ringInfo = ResponseHelper.<RingInfo>unpack(responseBuffer);//response.<RingInfo>unpack();
//
////        Assert.assertNotNull(ringInfo);
//    }


//    @Test
//    public void testNodeUp() throws Exception {
//        HostAndPort hostAndPort = HostAndPort.fromHost("127.0.0.1");
//        NodeInfo nodeInfo = new NodeInfo(hostAndPort, "1");
//
//        Ring ring = mock(Ring.class);
//
//        LocalNode localNode = mock(LocalNode.class);
//        when(localNode.getNodeInfo()).thenReturn(nodeInfo);
//        when(localNode.getRing()).thenReturn(ring);
//
//        when(ring.getRingInfo()).thenReturn(new RingInfo(nodeInfo, Ring.sort(Sets.newHashSet()), nodeInfo));
//
//        NodeFactory nodeFactory = mock(NodeFactory.class);
//        when(nodeFactory.get(any(HostAndPort.class))).thenReturn(localNode);
//
//        ServerEventsHandler serverEventsHandler = new ServerEventsHandler(localNode);
//
//        RequestBuffer requestBuffer = mock(RequestBuffer.class);
//        when(requestBuffer.getOperationCode()).thenReturn(OperationType.NODE_UP.getCode());
////        when(RequestHelper.unpack(request)).thenReturn(mock(NodeInfo.class));
//
//
////        ByteBuffer responseBuffer = serverEventsHandler.handle(requestBuffer);
////
////        verify(localNode).nodeUp(any(NodeInfo.class));
////        verify(requestBuffer).getOperationCode();
////
////
////        ResponseBuffer response = new ResponseBuffer(client, responseBuffer);
//
////        long id = response.getId();
////        RingInfo ringInfo = response.<RingInfo>unpack();
//
////        Assert.assertNotNull(ringInfo);
//    }
//
//    @Test
//    public void testNodeDown() throws Exception {
//        HostAndPort hostAndPort = HostAndPort.fromHost("127.0.0.1");
//        NodeInfo nodeInfo = new NodeInfo(hostAndPort, "1");
//
//        Ring ring = mock(Ring.class);
//
//        LocalNode localNode = mock(LocalNode.class);
//        when(localNode.getNodeInfo()).thenReturn(nodeInfo);
//        when(localNode.getRing()).thenReturn(ring);
//
//        when(ring.getRingInfo()).thenReturn(new RingInfo(nodeInfo, Ring.sort(Sets.newHashSet()), nodeInfo));
//
//        NodeFactory nodeFactory = mock(NodeFactory.class);
//        when(nodeFactory.get(any(HostAndPort.class))).thenReturn(localNode);
//
//        ServerEventsHandler serverEventsHandler = new ServerEventsHandler(localNode);
//
//        RequestBuffer requestBuffer = mock(RequestBuffer.class);
//        when(requestBuffer.getOperationCode()).thenReturn(OperationType.NODE_DOWN.getCode());
////        when(request.unpack()).thenReturn(mock(NodeInfo.class));
//
//
////        ByteBuffer responseBuffer = serverEventsHandler.handle(requestBuffer);
////
////        verify(localNode).nodeDown(any(NodeInfo.class));
////        verify(requestBuffer).getOperationCode();
////
////
////        ResponseBuffer response = new ResponseBuffer(client, responseBuffer);
////
////        long id = response.getId();
////        RingInfo ringInfo = response.<RingInfo>unpack();
//
////        Assert.assertEquals(StatusCode.OK, response.getStatusCode());
//    }
//
//    @Test
//    public void testElectCoordinator() throws Exception {
//        HostAndPort hostAndPort = HostAndPort.fromHost("127.0.0.1");
//        NodeInfo nodeInfo = new NodeInfo(hostAndPort, "1");
//
//        Ring ring = mock(Ring.class);
//
//        LocalNode localNode = mock(LocalNode.class);
//        when(localNode.getNodeInfo()).thenReturn(nodeInfo);
//        when(localNode.getRing()).thenReturn(ring);
//
//        when(ring.getRingInfo()).thenReturn(new RingInfo(nodeInfo, Ring.sort(Sets.newHashSet()), nodeInfo));
//
//        NodeFactory nodeFactory = mock(NodeFactory.class);
//        when(nodeFactory.get(any(HostAndPort.class))).thenReturn(localNode);
//
//        ServerEventsHandler serverEventsHandler = new ServerEventsHandler(localNode);
//
//        RequestBuffer requestBuffer = mock(RequestBuffer.class);
////        when(request.getOperationCode()).thenReturn(OperationType.ELECT_COORDINATOR);
////        when(request.unpack()).thenReturn(mock(Elect.class));
//
//
////        ByteBuffer responseBuffer = serverEventsHandler.handle(requestBuffer);
////
////        verify(localNode).elect(any());
////        verify(requestBuffer).getOperationCode();
////
////
////        ResponseBuffer response = new ResponseBuffer(client, responseBuffer);
//
////        long id = response.getId();
////
////        Assert.assertEquals(StatusCode.OK, response.getStatusCode());
//
//    }
//
//    @Test
//    public void testElectedCoordinator() throws Exception {
//        HostAndPort hostAndPort = HostAndPort.fromHost("127.0.0.1");
//        NodeInfo nodeInfo = new NodeInfo(hostAndPort, "1");
//
//        Ring ring = mock(Ring.class);
//
//        LocalNode localNode = mock(LocalNode.class);
//        when(localNode.getNodeInfo()).thenReturn(nodeInfo);
//        when(localNode.getRing()).thenReturn(ring);
//
//        when(ring.getRingInfo()).thenReturn(new RingInfo(nodeInfo, Ring.sort(Sets.newHashSet()), nodeInfo));
//
//        NodeFactory nodeFactory = mock(NodeFactory.class);
//        when(nodeFactory.get(any(HostAndPort.class))).thenReturn(localNode);
//
//        ServerEventsHandler serverEventsHandler = new ServerEventsHandler(localNode);
//
//        RequestBuffer requestBuffer = mock(RequestBuffer.class);
////        when(request.getOperationCode()).thenReturn(OperationType.ELECTED_COORDINATOR);
////        when(request.unpack()).thenReturn(mock(NodeInfo.class));
//
//
////        ByteBuffer responseBuffer = serverEventsHandler.handle(requestBuffer);
////
////        verify(localNode).elected(any());
////        verify(requestBuffer).getOperationCode();
////
////
////        ResponseBuffer response = new ResponseBuffer(client, responseBuffer);
//
////        long id = response.getId();
////        StatusCode statusCode = response.getStatusCode();
//
////        Assert.assertNotNull(ring);
////        Assert.assertNotNull(statusCode);
////        Assert.assertEquals(StatusCode.OK, statusCode);
//
//
//    }


    @After
    public void tearDown() throws Exception {
        Metrics.reporter.report();

    }
}