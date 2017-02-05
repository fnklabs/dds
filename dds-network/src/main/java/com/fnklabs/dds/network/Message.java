package com.fnklabs.dds.network;

import com.fnklabs.metrics.MetricsFactory;
import com.fnklabs.metrics.Timer;
import com.google.common.base.Verify;
import lombok.ToString;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Network message
 * <p>
 * <table>
 * <tr>
 * <th>Parameter</th>
 * <th>ID Data</th>
 * <th>REPLY MESSAGE ID</th>
 * <th>MESSAGE SIZE</th>
 * <th>STATUS CODE</th>
 * <th>API Version</th>
 * <th>Message DATA</th>
 * </tr>
 * <tr>
 * <th>Size (<i>in bytes</i>)</th>
 * <td>8</td>
 * <td>8</td>
 * <td>4</td>
 * <td>4</td>
 * <td>4</td>
 * <td>~</td>
 * </tr>
 * <tr>
 * <th>Range</th>
 * <td>0-7</td>
 * <td>8-15</td>
 * <td>16-19</td>
 * <td>20-23</td>
 * <td>24-27</td>
 * <td>28-N</td>
 * </tr>
 * </table>
 */
@ToString(of = {"id", "replyMessageId", "client", "statusCode", "apiVersion"})
class Message {

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

    /**
     * Message sequence ID
     */
    private static final AtomicLong ID_SEQUENCE = new AtomicLong(0);

    /**
     *
     */
    private static final long serialVersionUID = -2754921141679850688L;

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
    private ApiVersion apiVersion;
    /**
     * Message data
     */
    private byte[] messageData;

    /**
     *
     */
    private Message() {
    }

    /**
     * @param statusCode  Message status code. Set {@link StatusCode#OK} if it's not reply message
     * @param apiVersion  Api version. Use {@link ApiVersion#VERSION_1} constants
     * @param messageData Message data
     */
    public Message(StatusCode statusCode, ApiVersion apiVersion, @Nullable byte[] messageData) {
        this.id = ID_SEQUENCE.incrementAndGet();
        this.replyMessageId = 0;
        this.statusCode = statusCode;
        this.apiVersion = apiVersion;
        this.messageData = messageData;

        messageSize = HEADER_SIZE + (messageData != null ? messageData.length : 0);
    }

    /**
     * @param replyMessageId Reply message id, must be < 0 if message is not reply
     * @param statusCode     Message status code. Set {@link StatusCode#OK} if it's not reply message
     * @param apiVersion     Api version. Use {@link ApiVersion#VERSION_1} constants
     * @param messageData    Message data
     * @param client         Client id. User by server to identify to which client must be send message
     */
    public Message(long replyMessageId, StatusCode statusCode, ApiVersion apiVersion, @Nullable byte[] messageData, long client) {
        this.id = ID_SEQUENCE.incrementAndGet();
        this.replyMessageId = replyMessageId;
        this.statusCode = statusCode;
        this.apiVersion = apiVersion;
        this.messageData = messageData;
        this.client = client;

        messageSize = HEADER_SIZE + (messageData != null ? messageData.length : 0);
    }

    /**
     * @param replyMessageId Reply message id, must be < 0 if message is not reply
     * @param statusCode     Message status code. Set {@link StatusCode#OK} if it's not reply message
     * @param apiVersion     Api version. Use {@link ApiVersion#VERSION_1} constants
     * @param messageData    Message data
     */
    public Message(long replyMessageId, StatusCode statusCode, ApiVersion apiVersion, @Nullable byte[] messageData) {
        this.id = ID_SEQUENCE.incrementAndGet();
        this.replyMessageId = replyMessageId;
        this.statusCode = statusCode;
        this.apiVersion = apiVersion;
        this.messageData = messageData;

        messageSize = HEADER_SIZE + (messageData != null ? messageData.length : 0);
    }

    /**
     * Get next sequence id
     *
     * @return Next sequence id
     */
    public static long getNextId() {
        return ID_SEQUENCE.incrementAndGet();
    }

    public static int messageLength(ByteBuffer data) {
        return data.getInt(MESSAGE_SIZE_OFFSET);
    }

    public static ByteBuffer pack(Message message) {
        ByteBuffer buffer = ByteBuffer.allocate(message.getMessageSize());
        buffer.putLong(MESSAGE_ID_OFFSET, message.getId());
        buffer.putLong(REPLY_MESSAGE_ID_OFFSET, message.getReplyMessageId());
        buffer.putInt(MESSAGE_SIZE_OFFSET, message.getMessageSize());
        buffer.putInt(STATUS_CODE_OFFSET, message.getStatusCode().value());
        buffer.putInt(API_VERSION_OFFSET, message.getApiVersion().getVersion());

        if (message.getMessageData() != null) {
            buffer.position(DATA_OFFSET);
            buffer.put(message.getMessageData());
        }

        buffer.rewind();

        return buffer;
    }

    public static Message unpack(ByteBuffer buffer) {
        Timer timer = MetricsFactory.getMetrics().getTimer("network.message.unpack");

        int msgSize = Message.messageLength(buffer);

        Message message = new Message();

        if (msgSize == HEADER_SIZE) {
            message.id = buffer.getLong(MESSAGE_ID_OFFSET);
            message.replyMessageId = buffer.getLong(REPLY_MESSAGE_ID_OFFSET);
            message.statusCode = StatusCode.valueOf(buffer.getInt(STATUS_CODE_OFFSET));
            message.apiVersion = ApiVersion.valueOf(buffer.getInt(API_VERSION_OFFSET));

        } else {
            int dataLength = msgSize - HEADER_SIZE;

            Verify.verify(dataLength > 0);

            byte[] data = new byte[dataLength];

            for (int i = 0; i < dataLength; i++) {
                data[i] = buffer.get(DATA_OFFSET + i);
            }

            message.id = buffer.getLong(MESSAGE_ID_OFFSET);
            message.replyMessageId = buffer.getLong(REPLY_MESSAGE_ID_OFFSET);
            message.statusCode = StatusCode.valueOf(buffer.getInt(STATUS_CODE_OFFSET));
            message.apiVersion = ApiVersion.valueOf(buffer.getInt(API_VERSION_OFFSET));
            message.messageData = data;
        }

        timer.stop();

        return message;
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

    public ApiVersion getApiVersion() {
        return apiVersion;
    }

    public byte[] getMessageData() {
        return messageData;
    }
}
