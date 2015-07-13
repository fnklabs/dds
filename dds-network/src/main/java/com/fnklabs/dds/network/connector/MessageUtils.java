package com.fnklabs.dds.network.connector;

import com.codahale.metrics.Timer;
import com.fnklabs.dds.Metrics;
import com.fnklabs.dds.network.StatusCode;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;

public class MessageUtils {
    private static final Logger LOGGER = LoggerFactory.getLogger(MessageUtils.class);


    /**
     * Get operation type
     *
     * @param message Header part or message
     *
     * @return operation type
     */
    public static int getOperationType(@NotNull ByteBuffer message) {
        return message.getInt(MessageBuffer.OPERATION_TYPE_OFFSET);
    }

    /**
     * Get message id
     *
     * @param message Message byte buffer
     *
     * @return Message id
     */
    public static long getId(ByteBuffer message) {
        return message.getLong(MessageBuffer.MESSAGE_ID_OFFSET);
    }

    public static long getReplyMessageId(ByteBuffer responseBuffer) {
        return responseBuffer.getLong(MessageBuffer.REPLY_MESSAGE_ID_OFFSET);
    }

    /**
     * Get message size
     *
     * @param message Message buffer
     *
     * @return Size of message
     */
    public static int getMessageSize(ByteBuffer message) {
        return message.getInt(MessageBuffer.MESSAGE_SIZE_OFFSET);
    }

    /**
     * Message status code
     *
     * @param message Message buffer
     *
     * @return Message status code
     */
    public static StatusCode getStatusCode(ByteBuffer message) {
        int statusCode = message.getInt(MessageBuffer.STATUS_CODE_OFFSET);

        return StatusCode.valueOf(statusCode);
    }

    public static <T> Message<T> transform(MessageBuffer messageBuffer) {
        Timer.Context timer = Metrics.getTimer(Metrics.Type.NET_MESSAGE_TRANSFORM).time();

        NetworkMessage<T> networkMessage = new NetworkMessage<>();
        networkMessage.setClientId(messageBuffer.getClient());
        networkMessage.setId(messageBuffer.getId());
        networkMessage.setReplyMessageId(messageBuffer.getReplyMessageId());
        networkMessage.setStatusCode(messageBuffer.getStatusCode());
        networkMessage.setOperationCode(messageBuffer.getOperationCode());

        T unpackedObject = MessageUtils.<T>unpack(messageBuffer.getData());

        networkMessage.setData(unpackedObject);

        timer.stop();

        return networkMessage;
    }

    public static <T> MessageBuffer transform(Message<T> message) {
        Timer.Context timer = Metrics.getTimer(Metrics.Type.NET_MESSAGE_TRANSFORM).time();
        long id = message.getId();
        long replyMessageId = message.getReplyMessageId();
        int operationCode = message.getOperationCode();
        StatusCode statusCode = message.getStatusCode();
        T data = message.getData();

        MessageBuffer connectorMessageBuffer = new MessageBuffer(message.getClientId(), pack(id, replyMessageId, operationCode, statusCode, data));

        timer.stop();

        return connectorMessageBuffer;
    }

    private static ByteBuffer pack(long id, long replyMessage, StatusCode statusCode) {
        return pack(id, replyMessage, 0, statusCode);
    }

    private static <T> ByteBuffer pack(long id, long replyMessageId, int operationCode, StatusCode statusCode, T data) {
        try {
            ByteArrayOutputStream out = new ByteArrayOutputStream();
            ObjectOutputStream objectOutStream = new ObjectOutputStream(out);
            objectOutStream.writeObject(data);
            ByteBuffer serializedObject = ByteBuffer.wrap(out.toByteArray());

            objectOutStream.close();

            ByteBuffer buffer = ByteBuffer.allocate(MessageBuffer.HEADER_SIZE + serializedObject.limit());

            buffer.putLong(MessageBuffer.MESSAGE_ID_OFFSET, id);
            buffer.putLong(MessageBuffer.REPLY_MESSAGE_ID_OFFSET, replyMessageId);
            buffer.putInt(MessageBuffer.OPERATION_TYPE_OFFSET, operationCode);
            buffer.putInt(MessageBuffer.STATUS_CODE_OFFSET, statusCode.value());

            serializedObject.position(0);

            buffer.position(MessageBuffer.DATA_OFFSET);

            while (serializedObject.hasRemaining()) {
                buffer.put(serializedObject.get());
            }

            buffer.putInt(MessageBuffer.MESSAGE_SIZE_OFFSET, MessageBuffer.HEADER_SIZE + serializedObject.limit());

            buffer.position(0);

            return buffer;
        } catch (IOException e) {
            LOGGER.warn("Cant pack object", e);
        }


        return pack(id, replyMessageId, operationCode, StatusCode.CANT_PACK_MESSAGE);
    }

    private static <T> ByteBuffer pack(long id, long replyMessageId, int operationCode, StatusCode statusCode) {
        ByteBuffer buffer = ByteBuffer.allocate(MessageBuffer.HEADER_SIZE);

        buffer.putLong(MessageBuffer.MESSAGE_ID_OFFSET, id);
        buffer.putLong(MessageBuffer.REPLY_MESSAGE_ID_OFFSET, replyMessageId);
        buffer.putInt(MessageBuffer.OPERATION_TYPE_OFFSET, operationCode);
        buffer.putInt(MessageBuffer.STATUS_CODE_OFFSET, statusCode.value());
        buffer.putInt(MessageBuffer.MESSAGE_SIZE_OFFSET, MessageBuffer.HEADER_SIZE);

        buffer.position(0);

        return buffer;
    }

    /**
     * Unpack message from request (ByteBuffer)
     *
     * @param <T> Message class type
     *
     * @return Message object or null on error
     */
    @SuppressWarnings("Can produce OOM")
    @Nullable
    private static <T> T unpack(@NotNull ByteBuffer message) {
        Timer.Context timer = Metrics.getTimer(Metrics.Type.NET_MESSAGE_UNPACK).time();

        try {
            ObjectInputStream objectInputStream = new ObjectInputStream(new BufferedInputStream(new ByteArrayInputStream(message.array())));

            T t = (T) objectInputStream.readObject();

            objectInputStream.close();
            return t;
        } catch (IOException | ClassNotFoundException e) {
            LOGGER.warn("Cant unpack message", e);
        } finally {
            timer.stop();
        }

        return null;
    }


}
