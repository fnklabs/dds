package com.fnklabs.dds.network.connector;

import com.fnklabs.dds.network.StatusCode;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Base message interface
 * <p>
 * | ID DATA   | REPLY MESSAGE ID | MESSAGE SIZE | OPERATION TYPE |  STATUS CODE |  ADDITIONAL DATA |
 * | 8 bytes   | 8 bytes          | 4 bytes      |  4 bytes       |  4 bytes     |                  |
 * | 0-7 bytes | 8-15 bytes       | 16-19 bytes  |  20-23 bytes   |  24-27 bytes |  28-N            |
 */
public class MessageBuffer {
    /**
     * Message ID offset. Value can occupy 8 bytes (type is long)
     */
    final static int MESSAGE_ID_OFFSET = 0;
    /**
     * Message ID size in bytes.
     */
    final static int MESSAGE_ID_SIZE = Long.BYTES;
    /**
     * Reply message id offset
     */
    final static int REPLY_MESSAGE_ID_OFFSET = 8;
    /**
     * Reply message id size
     */
    final static int REPLY_MESSAGE_ID_SIZE = Long.BYTES;
    /**
     * Header parameter DATA_LENGTH start index. Value can occupy 4 bytes (type is int)
     */
    final static int MESSAGE_SIZE_OFFSET = 16;
    /**
     * Header parameter DATA_LENGTH size in bytes
     */
    final static int MESSAGE_SIZE_SIZE = Integer.BYTES;
    /**
     * Header parameter OperationType start index, value can occupy 4 bytes (type is int)
     */
    final static int OPERATION_TYPE_OFFSET = 20;
    /**
     * Header parameter OperationType size in bytes
     */
    final static int OPERATION_TYPE_SIZE = Integer.BYTES;
    /**
     * Status code offset
     */
    final static int STATUS_CODE_OFFSET = 24;
    /**
     * Status code size
     */
    final static int STATUS_CODE_SIZE = Integer.BYTES;
    final static int DATA_OFFSET = 28;
    final static int HEADER_SIZE = 28;
    private final long client;
    private ByteBuffer responseBuffer;
    private AtomicLong retrievedBytes = new AtomicLong(0);

    public MessageBuffer(long client) {
        this.client = client;
    }

    public MessageBuffer(long client, MappedByteBuffer responseBuffer) {
        this.client = client;
        this.responseBuffer = responseBuffer;
    }

    public MessageBuffer(long client, ByteBuffer buffer) {
        this.client = client;
        responseBuffer = buffer;
    }

    public long getClient() {
        return client;
    }

    public boolean isOk() {
        return getStatusCode() == StatusCode.OK;
    }

    /**
     * Get status code
     *
     * @return
     */
    public StatusCode getStatusCode() {
        return MessageUtils.getStatusCode(getResponseBuffer());
    }

    /**
     * Return id of reply message
     * <p>
     *
     * @return value < 0  if message is not reply
     */
    public long getReplyMessageId() {
        return MessageUtils.getReplyMessageId(getResponseBuffer());
    }

    /**
     * Return operation code
     *
     * @return operation code
     */
    public int getOperationCode() {
        return MessageUtils.getOperationType(getResponseBuffer());
    }

    /**
     * Return message unique id. Must be unique per session (connection)
     *
     * @return Message id
     */
    public long getId() {
        return MessageUtils.getId(getResponseBuffer());
    }

    /**
     * Get data length in bytes
     *
     * @return Length of data in bytes
     */
    public int getMessageSize() {
        return MessageUtils.getMessageSize(getResponseBuffer());
    }

    /**
     * Append data to requestBuffer if all data is received then force flush data to disk and flip request buffer
     *
     * @param data Data
     */
    public void append(ByteBuffer data) {
        while (data.hasRemaining()) {
            getResponseBuffer().put(data.get());
        }

        retrievedBytes.getAndAdd(data.limit());

        if (isFullyReceived()) {
            if (getResponseBuffer() instanceof MappedByteBuffer) {
                ((MappedByteBuffer) getResponseBuffer()).force();
            }
            getResponseBuffer().rewind();
        }
    }

    public boolean isFullyReceived() {
        return getMessageSize() <= retrievedBytes.get();
    }

    /**
     * Get additional data part
     * <p>
     * Will return empty Buffer if request doesn't contains data part
     *
     * @return Data part
     */
    @NotNull
    public ByteBuffer getData() {
        ByteBuffer responseBuffer = getResponseBuffer();
        responseBuffer.rewind();

        int dataSize = getDataSize();
        ByteBuffer dataBuffer = ByteBuffer.allocate(dataSize);

        for (int i = DATA_OFFSET; i < getMessageSize(); i++) {
            dataBuffer.put(responseBuffer.get(i));
        }

        getResponseBuffer().rewind();
        dataBuffer.rewind();

        return dataBuffer;
    }

    public ByteBuffer getResponseBuffer() {
        return responseBuffer;
    }


    /**
     * Get data size
     *
     * @return Data size
     */
    public int getDataSize() {
        return getMessageSize() - HEADER_SIZE;
    }


}
