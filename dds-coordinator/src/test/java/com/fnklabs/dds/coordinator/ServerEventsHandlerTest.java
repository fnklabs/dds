package com.fnklabs.dds.coordinator;

public class ServerEventsHandlerTest {

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
}