package com.fnklabs.dds.network;

import org.jetbrains.annotations.NotNull;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Base message interface
 * <p>
 * Parameter: | ID DATA   | REPLY MESSAGE ID | MESSAGE SIZE | STATUS CODE | API Version | Message DATA |
 * Size:      | 8 bytes   | 8 bytes          | 4 bytes      | 4 bytes     | 4 bytes     | 4 bytes      |
 * Range      | 0-7 bytes | 8-15 bytes       | 16-19 bytes  | 20-23 bytes | 24-27       | 28-N         |
 */
public class Message implements Externalizable {

    public static final int MAX_MESSAGE_SIZE = 5 * 1024 * 1024; // 5MB
    /**
     * Message ID offset. Value can occupy 8 bytes (type is long)
     */
    final static int MESSAGE_ID_OFFSET = 0;
    /**
     * Reply message id offset
     */
    final static int REPLY_MESSAGE_ID_OFFSET = 8;
    /**
     * Header parameter DATA_LENGTH start index. Value can occupy 4 bytes (type is int)
     */
    final static int MESSAGE_SIZE_OFFSET = 16;
    /**
     * Status code offset
     */

    final static int STATUS_CODE_OFFSET = 20;
    /**
     * Api version number start index
     */
    final static int API_VERSION_OFFSET = 24;
    /**
     * Position from which data will start
     */
    final static int DATA_OFFSET = 28;
    /**
     * Message header size in bytes
     */
    final static int HEADER_SIZE = 28;
    private static final AtomicLong ID_SEQUENCE = new AtomicLong(0);
    private static final long serialVersionUID = -8246409308426854643L;
    /**
     * Network Client id for internal use
     */
    private transient long client;
    /**
     * Message id
     */
    private long id;
    /**
     * Reply message id
     */
    private long replyMessageId;
    /**
     * Size of current message in bytes
     */
    private int messageSize;
    /**
     * Message status code
     */
    private StatusCode statusCode = StatusCode.OK;
    /**
     * Api version number
     */
    private int apiVersion;
    /**
     * Message data
     */
    private byte[] messageData;

    /**
     *
     */
    @SuppressWarnings("Used for serialization. Don't use current constructor")
    @Deprecated
    public Message() {
    }


    /**
     * @param id             Message id. Must be unique per session
     * @param replyMessageId Reply message id, must be < 0 if message is not reply
     * @param statusCode     Message status code. Set {@link StatusCode#OK} if it's not reply message
     * @param apiVersion     Api version. Use {@link ApiVersion#VERSION_1} constants
     * @param messageData    Message data
     * @param client         Client id. User by server to identify to which client must be send message
     */
    public Message(long id, long replyMessageId, @NotNull StatusCode statusCode, int apiVersion, @NotNull byte[] messageData, long client) {
        this.id = id;
        this.replyMessageId = replyMessageId;
        this.statusCode = statusCode;
        this.apiVersion = apiVersion;
        this.messageData = messageData;
        this.client = client;

        messageSize = HEADER_SIZE + messageData.length;
    }

    /**
     * Create reply message from specified message
     *
     * @param message     Message from which we must create reply
     * @param statusCode  Reply code
     * @param apiVersion  Api version
     * @param messageData Reply message data
     *
     * @return Reply message
     */
    public static <T> Message createReply(Message message, @NotNull StatusCode statusCode, int apiVersion, @NotNull T messageData) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(out);
            objectOutputStream.writeObject(messageData);

            return new Message(getNextId(), message.getId(), statusCode, apiVersion, out.toByteArray(), message.getClient());
        } catch (IOException e) {
            LoggerFactory.getLogger(Message.class).warn("Can't serialize input object", e);
        }

        return new Message(getNextId(), message.getId(), StatusCode.UNKNOWN, apiVersion, new byte[0], message.getClient());
    }

    /**
     * Create reply message from specified message
     *
     * @param message     Message from which we must create reply
     * @param statusCode  Reply code
     * @param apiVersion  Api version
     * @param messageData Reply message data
     *
     * @return Reply message
     */
    public static Message createReply(Message message, @NotNull StatusCode statusCode, int apiVersion, @NotNull byte[] messageData) {
        return new Message(getNextId(), message.getId(), statusCode, apiVersion, messageData, message.getClient());
    }

    /**
     * Get next sequence id
     *
     * @return Next sequence id
     */
    public static long getNextId() {
        return ID_SEQUENCE.incrementAndGet();
    }

    public long getClient() {
        return client;
    }

    public void setClient(long client) {
        this.client = client;
    }

    public boolean isOk() {
        return getStatusCode() == StatusCode.OK;
    }

    /**
     * Return message unique id. Must be unique per session (connection)
     *
     * @return Message id
     */
    public long getId() {
        return id;
    }

    /**
     * Get status code
     *
     * @return
     */
    public StatusCode getStatusCode() {
        return statusCode;
    }


    /**
     * Return id of reply message
     * <p>
     *
     * @return value < 0  if message is not reply
     */
    public long getReplyMessageId() {
        return replyMessageId;
    }

    /**
     * Get data length in bytes
     *
     * @return Length of data in bytes
     */
    public int getMessageSize() {
        return messageSize;
    }

    public int getApiVersion() {
        return apiVersion;
    }

    public byte[] getMessageData() {
        return messageData;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(getId());
        out.writeLong(getReplyMessageId());
        out.writeInt(getMessageSize());
        out.writeInt(getStatusCode().value());
        out.writeInt(getApiVersion());

        if (messageData != null) {
            out.write(messageData);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        id = in.readLong();
        replyMessageId = in.readLong();
        messageSize = in.readInt();
        statusCode = StatusCode.valueOf(in.readInt());
        apiVersion = in.readInt();

        if (messageSize != HEADER_SIZE) {
            messageData = new byte[messageSize - HEADER_SIZE];
            in.readFully(messageData);
        }

    }


}
