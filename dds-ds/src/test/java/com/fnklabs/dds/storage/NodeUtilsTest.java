package com.fnklabs.dds.storage;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.math.BigInteger;
import java.nio.ByteBuffer;

import static org.junit.Assert.*;

public class NodeUtilsTest {

    private ByteBuffer BYTE_BUFFER;

    @Before
    public void setUp() throws Exception {
        BYTE_BUFFER = NodeUtils.createByteBuffer(Integer.BYTES);
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void writeKeyLength() throws Exception {
        NodeUtils.writeKeyLength(4, BYTE_BUFFER);

        Assert.assertEquals(0, BYTE_BUFFER.position());
        Assert.assertEquals(4, BYTE_BUFFER.getInt(NodeUtils.KEY_LENGTH_POSITION));
    }

    @Test
    public void readKeyLength() throws Exception {
        BYTE_BUFFER.putInt(NodeUtils.KEY_LENGTH_POSITION, 4);

        int keyLength = NodeUtils.readKeyLength(BYTE_BUFFER);

        Assert.assertEquals(0, BYTE_BUFFER.position());
        Assert.assertEquals(4, keyLength);
    }


    @Test
    public void writePosition() throws Exception {
        NodeUtils.writePosition(3, BYTE_BUFFER);

        Assert.assertEquals(0, BYTE_BUFFER.position());
        Assert.assertEquals(3, BYTE_BUFFER.getLong(NodeUtils.POSITION_INDEX));
    }

    @Test
    public void readPosition() throws Exception {
        BYTE_BUFFER.putLong(NodeUtils.POSITION_INDEX, 3);

        long position = NodeUtils.readPosition(BYTE_BUFFER);

        Assert.assertEquals(0, BYTE_BUFFER.position());
        Assert.assertEquals(3, position);

    }


    @Test
    public void writeDataReference() throws Exception {
        NodeUtils.writeDataReference(3, BYTE_BUFFER);

        Assert.assertEquals(0, BYTE_BUFFER.position());
        Assert.assertEquals(3, BYTE_BUFFER.getLong(NodeUtils.DATA_REFERENCE_POSITION));
    }

    @Test
    public void readDataReference() throws Exception {
        BYTE_BUFFER.putLong(NodeUtils.DATA_REFERENCE_POSITION, 3);

        long value = NodeUtils.readDataReference(BYTE_BUFFER);

        Assert.assertEquals(0, BYTE_BUFFER.position());
        Assert.assertEquals(3, value);
    }

    @Test
    public void writeDepth() throws Exception {
        NodeUtils.writeDepth(3, BYTE_BUFFER);

        Assert.assertEquals(0, BYTE_BUFFER.position());
        Assert.assertEquals(3, BYTE_BUFFER.getLong(NodeUtils.DEPTH_POSITION));
    }

    @Test
    public void readDepth() throws Exception {
        BYTE_BUFFER.putLong(NodeUtils.DEPTH_POSITION, 3);

        long value = NodeUtils.readDepth(BYTE_BUFFER);

        Assert.assertEquals(0, BYTE_BUFFER.position());
        Assert.assertEquals(3, value);
    }

    @Test
    public void writeLeftNodePosition() throws Exception {
        NodeUtils.writeLeftNodePosition(3, BYTE_BUFFER);

        Assert.assertEquals(0, BYTE_BUFFER.position());
        Assert.assertEquals(3, BYTE_BUFFER.getLong(NodeUtils.LEFT_NODE_POSITION));
    }

    @Test
    public void readLeftNodePosition() throws Exception {
        BYTE_BUFFER.putLong(NodeUtils.LEFT_NODE_POSITION, 3);

        long value = NodeUtils.readLeftNodePosition(BYTE_BUFFER);

        Assert.assertEquals(0, BYTE_BUFFER.position());
        Assert.assertEquals(3, value);
    }

    @Test
    public void writeRightNodePosition() throws Exception {
        NodeUtils.writeRightNodePosition(3, BYTE_BUFFER);

        Assert.assertEquals(0, BYTE_BUFFER.position());
        Assert.assertEquals(3, BYTE_BUFFER.getLong(NodeUtils.RIGHT_NODE_POSITION));
    }

    @Test
    public void readRightNodePosition() throws Exception {
        BYTE_BUFFER.putLong(NodeUtils.RIGHT_NODE_POSITION, 3);

        long value = NodeUtils.readRightNodePosition(BYTE_BUFFER);

        Assert.assertEquals(0, BYTE_BUFFER.position());
        Assert.assertEquals(3, value);
    }

    @Test
    public void getBufferLength() throws Exception {
        int bufferLength = NodeUtils.getBufferLength(1);

        Assert.assertEquals(NodeUtils.HEADER_LENGTH + 1, bufferLength);
    }


    @Test
    public void createByteBuffer() throws Exception {
        ByteBuffer byteBuffer = NodeUtils.createByteBuffer(1);

        Assert.assertEquals(0, byteBuffer.position());
        Assert.assertEquals(NodeUtils.getBufferLength(1), byteBuffer.limit());
    }

    @Test
    public void fromByteBuffer() throws Exception {
        Node nodeInfo = NodeUtils.fromByteBuffer(BYTE_BUFFER);

//        Assert.assertEquals(0, BytesUtils.compare(node.getKey(), nodeInfo.getKey()));
//        Assert.assertEquals(node.getDataReference().get(), nodeInfo.getDataReference().get());
//        Assert.assertEquals(node.getPosition().get(), nodeInfo.getPosition().get());
//        Assert.assertEquals(node.getLeftNodeReference().get(), nodeInfo.getLeftNodeReference().get());
//        Assert.assertEquals(node.getRightNodeReference().get(), nodeInfo.getRightNodeReference().get());
    }

    @Test
    public void toByteBuffer() throws Exception {
        BigInteger key = new BigInteger("1234");
        int keyLength = key.toByteArray().length;

        Node node = Node.builder()
                        .withKey(key.toByteArray())
                        .withPosition(1L)
                        .withDataReference(2L)
                        .withDepth(8)
                        .withLeftNodeReference(3L)
                        .withRightNodeReference(4L)
                        .build();

        ByteBuffer buffer = NodeUtils.createByteBuffer(keyLength);
        NodeUtils.writeToBuffer(node, buffer);

        Assert.assertEquals(NodeUtils.createByteBuffer(keyLength).limit(), buffer.limit());

        Assert.assertEquals(0, buffer.position());
        Assert.assertEquals(NodeUtils.getBufferLength(keyLength), buffer.limit());


    }
}