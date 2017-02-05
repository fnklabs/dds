package com.fnklabs.dds.storage;

import java.nio.ByteBuffer;

/**
 * Head
 * | KEY LENGTH (int) | Position (long) | Data reference (long) | Depth (long) | Left Node reference (long) | Right Node reference(long) | KEY byte[key length] |
 */
final class NodeUtils {
    static final int KEY_LENGTH_POSITION = 0;
    static final int KEY_LENGTH_SIZE = Integer.BYTES;

    static final int POSITION_INDEX = KEY_LENGTH_POSITION + KEY_LENGTH_SIZE;
    static final int POSITION_SIZE = Long.BYTES;

    static final int DATA_REFERENCE_POSITION = POSITION_INDEX + POSITION_SIZE;
    static final int DATA_REFERENCE_SIZE = Integer.BYTES;

    static final int DEPTH_POSITION = DATA_REFERENCE_POSITION + DATA_REFERENCE_SIZE;
    static final int DEPTH_SIZE = Long.BYTES;

    static final int LEFT_NODE_POSITION = DEPTH_POSITION + DEPTH_SIZE;
    static final int LEFT_NODE_POSITION_SIZE = Long.BYTES;

    static final int RIGHT_NODE_POSITION = LEFT_NODE_POSITION + LEFT_NODE_POSITION_SIZE;
    static final int RIGHT_NODE_POSITION_SIZE = Long.BYTES;

    static final int KEY_POSITION = RIGHT_NODE_POSITION + RIGHT_NODE_POSITION_SIZE;

    static final int HEADER_LENGTH = KEY_LENGTH_SIZE + POSITION_SIZE + DATA_REFERENCE_SIZE + DEPTH_SIZE + LEFT_NODE_POSITION_SIZE + RIGHT_NODE_POSITION_SIZE;

    static void writeKeyLength(int keyLength, ByteBuffer buffer) {
        buffer.putInt(KEY_LENGTH_POSITION, keyLength);
    }

    static int readKeyLength(ByteBuffer buffer) {
        return buffer.getInt(KEY_LENGTH_POSITION);
    }

    static void writePosition(long position, ByteBuffer buffer) {
        buffer.putLong(POSITION_INDEX, position);
    }

    static long readPosition(ByteBuffer buffer) {
        return buffer.getLong(POSITION_INDEX);
    }

    static void writeDataReference(long dataReference, ByteBuffer byteBuffer) {
        byteBuffer.putLong(DATA_REFERENCE_POSITION, dataReference);
    }

    static long readDataReference(ByteBuffer buffer) {
        return buffer.getLong(DATA_REFERENCE_POSITION);
    }

    static void writeDepth(long depth, ByteBuffer byteBuffer) {
        byteBuffer.putLong(DEPTH_POSITION, depth);
    }

    static long readDepth(ByteBuffer buffer) {
        return buffer.getLong(DEPTH_POSITION);
    }

    static void writeLeftNodePosition(long leftNodePosition, ByteBuffer byteBuffer) {
        byteBuffer.putLong(LEFT_NODE_POSITION, leftNodePosition);
    }

    static long readLeftNodePosition(ByteBuffer byteBuffer) {
        return byteBuffer.getLong(LEFT_NODE_POSITION);
    }

    static void writeRightNodePosition(long rightNodePosition, ByteBuffer byteBuffer) {
        byteBuffer.putLong(RIGHT_NODE_POSITION, rightNodePosition);
    }

    static long readRightNodePosition(ByteBuffer byteBuffer) {
        return byteBuffer.getLong(RIGHT_NODE_POSITION);
    }

    static void writeKey(byte[] key, ByteBuffer buffer) {
        int currentPosition = buffer.position();

        buffer.position(KEY_POSITION);

        buffer.put(key);

        buffer.position(currentPosition);
    }

    static byte[] getKey(int keyLength, ByteBuffer buffer) {
        byte[] key = new byte[keyLength];

        int currentPosition = buffer.position();

        buffer.position(KEY_POSITION);

        buffer.get(key);

        buffer.position(currentPosition);

        return key;
    }


    public static int getBufferLength(int keySize) {
        return HEADER_LENGTH + keySize;
    }


    static ByteBuffer createByteBuffer(int keyLength) {
        return ByteBuffer.allocate(getBufferLength(keyLength)); // key length + current position + data position + left node position + right node position
    }

    static Node fromByteBuffer(ByteBuffer buffer) {
        int keyLength = NodeUtils.readKeyLength(buffer);
        long position = NodeUtils.readPosition(buffer);
        long dataReference = NodeUtils.readDataReference(buffer);
        long leftNodePosition = NodeUtils.readLeftNodePosition(buffer);
        long rightNodePosition = NodeUtils.readRightNodePosition(buffer);

        return Node.builder()
                   .withKey(NodeUtils.getKey(keyLength, buffer))
                   .withPosition(position == -1 ? null : position)
                   .withDataReference(dataReference == -1 ? null : dataReference)
                   .withDepth(NodeUtils.readDepth(buffer))
                   .withLeftNodeReference(leftNodePosition == -1 ? null : leftNodePosition)
                   .withRightNodeReference(rightNodePosition == -1 ? null : rightNodePosition)
                   .build();
    }

    static void writeToBuffer(Node node, ByteBuffer byteBuffer) {
        int keyLength = node.getKey().length;

        NodeUtils.writeKeyLength(keyLength, byteBuffer);
        NodeUtils.writePosition(node.getPosition().orElse(-1L), byteBuffer);
        NodeUtils.writeDataReference(node.getDataReference().orElse(-1L), byteBuffer);
        NodeUtils.writeDepth(node.getDepth(), byteBuffer);
        NodeUtils.writeLeftNodePosition(node.getLeftNodeReference().orElse(-1L), byteBuffer);
        NodeUtils.writeRightNodePosition(node.getRightNodeReference().orElse(-1L), byteBuffer);
        NodeUtils.writeKey(node.getKey(), byteBuffer);

        byteBuffer.position(0);
    }

    static Node merge(Node oldNode, Node newNode) {
        return Node.builder()
                   .withKey(oldNode.getKey())
                   .withPosition(oldNode.getPosition().orElse(null))
                   .withDataReference(newNode.getDataReference().orElse(null))
                   .withDepth(oldNode.getDepth())
                   .withLeftNodeReference(newNode.getLeftNodeReference().orElse(null))
                   .withRightNodeReference(newNode.getRightNodeReference().orElse(null))
                   .build();
    }
}
