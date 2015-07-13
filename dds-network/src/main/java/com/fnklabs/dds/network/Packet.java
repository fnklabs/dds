package com.fnklabs.dds.network;

import com.codahale.metrics.Timer;
import com.fnklabs.dds.Metrics;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * Data window part
 * <p>
 * Format:
 * <p>
 * Size:     | 8  |    4     | total length - 12 |
 * Comments: | ID | SEQUENCE |        DATA       |
 */
public class Packet {
    public static int SIZE = 512; // 512 BYTES

    public static int ID_SIZE = 8;
    public static int SEQUENCE_SIZE = 4;
    public static int DATA_SIZE = SIZE - SEQUENCE_SIZE - ID_SIZE;

    /**
     * Packet id sequence
     */
    private static AtomicLong idSequence = new AtomicLong(0);

    /**
     * Packet ID
     */
    private long id;

    /**
     * Window sequence id
     */
    private int sequence;

    /**
     * Packet part data
     */
    private ByteBuffer data;

    public Packet(long id, int sequence, ByteBuffer data) {
        this.id = id;
        this.sequence = sequence;
        this.data = data;
    }

    /**
     * Unpack window into Window object
     *
     * @param window Window data
     *
     * @return Window object
     */
    public static Packet unpack(ByteBuffer window) {
        long id = window.getLong(0);
        int sequence = window.getInt(8);
        window.position(12);

        ByteBuffer data = createDataBufferInstance();

        while (window.hasRemaining()) {
            data.put(window.get());
        }

        data.rewind();

        return new Packet(id, sequence, data);
    }

    /**
     * Get window packet id
     *
     * @param packet Packet data
     *
     * @return Window packet id
     */
    public static long getId(ByteBuffer packet) {
        return packet.getLong(0);
    }

    /**
     * Get window sequence id
     *
     * @param window Window data
     *
     * @return Sequence id
     */
    public static int getSequence(ByteBuffer window) {
        return window.getInt(8);
    }

    /**
     * Get data from window
     *
     * @param window Window data
     *
     * @return
     */
    public static ByteBuffer getData(ByteBuffer window) {
        ByteBuffer data = createDataBufferInstance();

        for (int i = 12; i < SIZE; i++) {
            data.put(window.get(i));
        }

        data.flip();

        return data;
    }

    /**
     * Split data into windows and then send each part to consumer
     *
     * @param data     Message data
     * @param consumer Window consumer
     */
    public static void splitToPacket(ByteBuffer data, Consumer<ByteBuffer> consumer) {
        Timer.Context timer = Metrics.getTimer(Metrics.Type.NET_WINDOW_SPLIT).time();

        long id = getNextId();

        ByteBuffer buffer = createDataBufferInstance();

        int readBytes = 0;
        int sequence = 0;

        while (data.hasRemaining()) {
            buffer.put(data.get());

            readBytes++;

            if (readBytes % Packet.DATA_SIZE == 0) {
                buffer.rewind();

                ByteBuffer windowBuffer = Packet.pack(id, sequence, buffer);

                consumer.accept(windowBuffer);


                sequence++;

                buffer = createDataBufferInstance();

                readBytes = 0;
            }
        }

        if (readBytes > 0) {
            buffer.rewind();
            ByteBuffer windowBuffer = Packet.pack(id, sequence, buffer);
            consumer.accept(windowBuffer);
        }

        timer.stop();
    }

    /**
     * Retrieve window packet id
     *
     * @return id
     */
    public long getId() {
        return id;
    }

    /**
     * Get window sequence id
     *
     * @return sequence id
     */
    public int getSequence() {
        return sequence;
    }

    /**
     * Get window data
     *
     * @return window data
     */
    public ByteBuffer getData() {
        return data;
    }

    /**
     * Create data buffer instance
     *
     * @return Buffer
     */
    protected static ByteBuffer createDataBufferInstance() {
        return ByteBuffer.allocate(Packet.DATA_SIZE);
    }

    /**
     * Pack data. If data length greater than WINDOW data size then {@link #splitToPacket(ByteBuffer, Consumer)} must be called
     *
     * @param packetId Packet id
     * @param sequence Packet part sequence
     * @param data     Packet part data
     *
     * @return PacketPark
     */
    protected static ByteBuffer pack(long packetId, int sequence, ByteBuffer data) {
        ByteBuffer buffer = ByteBuffer.allocate(SIZE);
        buffer.putLong(0, packetId);
        buffer.putInt(8, sequence);

        buffer.position(12);

        while (data.hasRemaining()) {
            buffer.put(data.get());
        }

        buffer.rewind();

        return buffer;
    }

    /**
     * Get next packet id
     *
     * @return id
     */
    private static long getNextId() {
        return idSequence.incrementAndGet();
    }
}
