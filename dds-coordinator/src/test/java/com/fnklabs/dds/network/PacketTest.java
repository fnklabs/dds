package com.fnklabs.dds.network;

import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

public class PacketTest {

    static String simpleText;
    static String richText = "";

    @Before
    public void setUp() throws Exception {
        simpleText = "abc";

        for (int i = 0; i < 533; i++) {
            richText += i % 10;
        }
    }

    @Test
    public void testSplitSmallDataToWindows() throws Exception {

        ByteBuffer buffer = ByteBuffer.wrap(simpleText.getBytes());
        buffer.rewind();

        StringBuilder readData = new StringBuilder("");

        AtomicInteger parts = new AtomicInteger(0);

        Packet.splitToPacket(buffer, new Consumer<ByteBuffer>() {
            @Override
            public void accept(ByteBuffer byteBuffer) {
                parts.incrementAndGet();

                ByteBuffer data = Packet.getData(byteBuffer);

                String str = new String(data.array());
                StringUtils.strip(str);

                readData.append(StringUtils.trim(str));
            }
        });

        Assert.assertEquals(1, parts.get());
        Assert.assertEquals(3, readData.length());
        Assert.assertEquals(readData.toString(), simpleText);


    }

    @Test
    public void testSplitBigDataToWindows() throws Exception {

        ByteBuffer buffer = ByteBuffer.wrap(richText.getBytes());
        buffer.rewind();

        StringBuilder readData = new StringBuilder("");


        AtomicInteger parts = new AtomicInteger(0);

        Packet.splitToPacket(buffer, new Consumer<ByteBuffer>() {
            @Override
            public void accept(ByteBuffer byteBuffer) {
                parts.incrementAndGet();

                ByteBuffer data = Packet.getData(byteBuffer);

                String str = new String(data.array());
                StringUtils.strip(str);

                readData.append(StringUtils.trim(str));
            }
        });

        int i = richText.length() / Packet.DATA_SIZE + 1;
        Assert.assertEquals(i, parts.get());
        Assert.assertEquals(richText.length(), readData.toString().length());
        Assert.assertEquals(readData.toString(), richText);


    }

    @Test
    public void testPack() throws Exception {
        ByteBuffer buf = Packet.pack(99, 77, ByteBuffer.wrap("abc".getBytes()));

        Assert.assertEquals(Packet.SIZE, buf.limit());
        Assert.assertEquals(0, buf.position());


        long id = Packet.getId(buf);
        int sequence = Packet.getSequence(buf);
        ByteBuffer data = Packet.getData(buf);

        Assert.assertEquals(99, id);
        Assert.assertEquals(77, sequence);
        Assert.assertEquals(Packet.DATA_SIZE, data.limit());

    }
}