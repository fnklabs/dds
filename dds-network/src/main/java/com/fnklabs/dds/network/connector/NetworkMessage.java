package com.fnklabs.dds.network.connector;

import com.fnklabs.dds.network.StatusCode;

public class NetworkMessage<T> implements Message<T> {
    public static final long NOT_REPLY_MESSAGE = 0;
    private long replyMessageId = NOT_REPLY_MESSAGE;
    private long id;
    private long clientId;

    private int operationCode;

    private StatusCode statusCode = StatusCode.OK;

    private T data;

    @Override
    public long getReplyMessageId() {
        return replyMessageId;
    }

    public void setReplyMessageId(long replyMessageId) {
        if (replyMessageId < 0) {
            throw new RuntimeException("Reply message id must be greater than 0"); // todo refactor
        }
        this.replyMessageId = replyMessageId;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public long getClientId() {
        return clientId;
    }

    public void setClientId(long clientId) {
        this.clientId = clientId;
    }

    @Override
    public int getOperationCode() {
        return operationCode;
    }

    public void setOperationCode(int operationCode) {
        this.operationCode = operationCode;
    }

    @Override
    public StatusCode getStatusCode() {
        return statusCode;
    }

    public void setStatusCode(StatusCode statusCode) {
        this.statusCode = statusCode;
    }

    public T getData() {
        return data;
    }

    public void setData(T data) {
        this.data = data;
    }
}
