package com.fnklabs.dds.network;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Response structure
 * <pre>
 *     | ID   | Reply ID | API_VERSION | Data SIZE | Status Code |Extra DATA |
 *     |------|----------|-------------|-----------|-------------|-----------|
 *     | Long |  Long    | Integer     | Integer   | Integer     |  byte[]   |
 *     |------|----------|-------------|-----------|-------------|-----------|
 *     | 0-7  | 8-15     |  15-19      |  20-23    | 24-27       | 28 + SIZE |
 * </pre>
 */
public class ReplyMessage implements Message {
    public static final AtomicLong ID = new AtomicLong();

    private long id;

    private ApiVersion apiVersion;

    private int size;

    private long replyId;

    private StatusCode statusCode;

    private byte[] data;

    public long getReplyId() {
        return replyId;
    }

    public StatusCode getStatusCode() {
        return statusCode;
    }

    public ReplyMessage() {
    }

    public ReplyMessage(long id, long replyId, ApiVersion apiVersion, byte[] data) {
        this.id = id;
        this.apiVersion = apiVersion;
        this.replyId = replyId;
        this.statusCode = StatusCode.OK;
        this.data = data;
        this.size = data.length;
    }

    public ReplyMessage(long id, long replyId, ApiVersion apiVersion, StatusCode statusCode, byte[] data) {
        this.id = id;
        this.apiVersion = apiVersion;
        this.replyId = replyId;
        this.statusCode = statusCode;
        this.data = data;
        this.size = data.length;
    }

    @Override
    public long getId() {
        return id;
    }

    @Override
    public ApiVersion getVersion() {
        return apiVersion;
    }

    @Override
    public int getDataSize() {
        return size;
    }

    @Override
    public int getSize() {
        return Long.BYTES // id
                + Long.BYTES // reply id
                + Integer.BYTES // version
                + Integer.BYTES // size
                + Integer.BYTES // status code
                + getDataSize(); // data
    }

    @Override
    public byte[] getData() {
        return data;
    }

    @Override
    public void read(ByteBuffer buffer) {
        buffer.putLong(id);
        buffer.putLong(replyId);
        buffer.putInt(apiVersion.getVersion());
        buffer.putInt(size);
        buffer.putInt(statusCode.value());
        buffer.put(data);
    }

    @Override
    public void write(ByteBuffer buffer) {
        id = buffer.getLong();
        replyId = buffer.getLong();
        apiVersion = ApiVersion.valueOf(buffer.getInt());
        size = buffer.getInt();
        statusCode = StatusCode.valueOf(buffer.getInt());
        data = new byte[size];

        buffer.get(data);
    }

    public long replyId() {
        return replyId;
    }
}
