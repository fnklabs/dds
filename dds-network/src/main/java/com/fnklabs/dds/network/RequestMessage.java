package com.fnklabs.dds.network;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Base message structure
 * <pre>
 *     | ID   | API_VERSION | SIZE    | DATA           |
 *     |------|-------------|---------|----------------|
 *     | Long | Integer     | Integer |  byte[]        |
 *     |------|-------------|---------|----------------|
 *     | 0-7  | 8-11        | 12 - 15 | 16 - 16 + SIZE |
 * </pre>
 */
public class RequestMessage implements Message {
    public static final AtomicLong ID = new AtomicLong();

    private long id;

    private ApiVersion apiVersion;

    private int dataSize;

    private byte[] data;

    private long sessionId;

    public RequestMessage() {
    }

    @Override
    public String toString() {
        return "RequestMessage{" +
                "id=" + id +
                ", apiVersion=" + apiVersion +
                ", sessionId=" + sessionId +
                '}';
    }

    public RequestMessage(long id, ApiVersion apiVersion, byte[] data) {
        this.id = id;
        this.apiVersion = apiVersion;
        this.data = data;
        this.dataSize = data != null ? data.length : 0;
    }

    public long getSessionId() {
        return sessionId;
    }

    public void setSessionId(long sessionId) {
        this.sessionId = sessionId;
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
        return dataSize;
    }

    @Override
    public int getSize() {
        return Long.BYTES + Integer.BYTES + Integer.BYTES + getDataSize();
    }

    @Override
    public byte[] getData() {
        return data;
    }

    @Override
    public void read(ByteBuffer buffer) {
        buffer.putLong(id);
        buffer.putInt(apiVersion.getVersion());
        buffer.putInt(dataSize);
        buffer.put(data);
    }

    @Override
    public void write(ByteBuffer buffer) {
        id = buffer.getLong();
        apiVersion = ApiVersion.valueOf(buffer.getInt());
        dataSize = buffer.getInt();
        data = new byte[dataSize];

        buffer.get(data);

    }
}
