package com.fnklabs.dds.network;


public class RequestBufferTest {

//    @Test
//    public void testGetId() throws Exception {
//        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES + Long.BYTES);
//        buffer.putLong(89);
//        buffer.putChar('1');
//
//        Assert.assertEquals(89, RequestHelper.getId(buffer));
//
//    }
//
//    @Test
//    public void testGetMessageLength() throws Exception {
//        ByteBuffer buffer = ByteBuffer.allocate(RequestBuffer.HEADER_SIZE);
//        buffer.putLong(89);
//        buffer.putInt(1000002002);
//
//        Assert.assertEquals(1000002002, RequestHelper.getMessageSize(buffer));
//    }
//
//    @Test
//    public void testGetDataSize() throws Exception {
//        ByteBuffer buffer = ByteBuffer.allocate(RequestBuffer.HEADER_SIZE);
//        buffer.putLong(89);
//        buffer.putInt(1000002002);
//
//        Assert.assertEquals(1000002002 - RequestBuffer.HEADER_SIZE, RequestHelper.getDataSize(buffer));
//    }
//
//    @Test
//    public void testGetOperationType() throws Exception {
//        ByteBuffer buffer = ByteBuffer.allocate(RequestBuffer.HEADER_SIZE);
//        buffer.putLong(89);
//        buffer.putInt(1000002002);
//        buffer.putInt(RequestBuffer.OPERATION_TYPE_INDEX, OperationType.CLUSTER_INFO.getValue());
//
//        Assert.assertEquals(OperationType.CLUSTER_INFO, RequestHelper.getOperationCode(buffer));
//    }
//
//    @Test
//    public void testPack() throws Exception {
//        ByteBuffer pack = RequestHelper.pack(2, OperationType.CLUSTER_INFO.getValue(), ByteBuffer.wrap("ABC".getBytes()));
//
//        Assert.assertEquals(2, RequestHelper.getId(pack));
//        Assert.assertEquals(OperationType.CLUSTER_INFO.getCode(), RequestHelper.getOperationCode(pack));
//        Assert.assertEquals("ABC", new String(RequestHelper.getData(pack).array()));
//
//    }
//
//    @Test
//    public void testUnpack() throws Exception {
//        ByteArrayOutputStream out = new ByteArrayOutputStream();
//        ObjectOutputStream objectOutputStream = new ObjectOutputStream(out);
//
//        objectOutputStream.writeObject(new TestExample(3));
//
//
//        ByteBuffer buf = ByteBuffer.wrap(out.toByteArray());
//        buf.rewind();
//
//        ByteBuffer pack = RequestHelper.pack(2, OperationType.CLUSTER_INFO.getValue(), buf);
//
//
//        RequestBuffer requestBuffer = new RequestBuffer(1, pack);
//
//        TestExample unpack = RequestHelper.<TestExample>unpack(requestBuffer.getData());
//
//        Assert.assertEquals(3, unpack.getId());
//    }
//
//    static class TestExample implements Serializable {
//        private int id;
//
//        public TestExample(int id) {
//            this.id = id;
//        }
//
//        public int getId() {
//            return id;
//        }
//    }
}