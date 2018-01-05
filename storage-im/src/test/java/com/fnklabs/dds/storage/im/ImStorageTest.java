package com.fnklabs.dds.storage.im;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

public class ImStorageTest {

    private ImStorage imStorage;

    private ByteBuffer data;

    private ByteBuffer buffer;

    @Before
    public void setUp() throws Exception {
        imStorage = new ImStorage(64 * 1024 * 1024, 128); // 64 MB

        data = ByteBuffer.allocate(Long.BYTES + Integer.BYTES);
        data.putLong(156L);
        data.putInt(2);

        data.rewind();

        buffer = ByteBuffer.allocate(Long.BYTES + Integer.BYTES);
    }

    @Test
    public void write() {
        imStorage.write(0, data);

        imStorage.read(0, buffer);

        buffer.rewind();

        Assert.assertEquals(156L, buffer.getLong());
        Assert.assertEquals(2, buffer.getInt());
    }

    @Test
    public void readNone() {
        imStorage.read(0, buffer);

        buffer.rewind();

        Assert.assertEquals(0, buffer.getLong());
        Assert.assertEquals(0, buffer.getInt());
    }

    @Test
    public void readInt() {
        ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);

        imStorage.read(4, buffer);

        buffer.rewind();

        Assert.assertEquals(0, buffer.getInt());
    }

    @Test
    public void scanAll() {
        int blockSize = Long.BYTES + Integer.BYTES;

        for (int i = 0; i < blockSize * 100_000; i += blockSize) {
            imStorage.write(i, data);
            data.rewind();
        }

        AtomicInteger count = new AtomicInteger(0);

        imStorage.scan(
                0,
                (position, buffer) -> {
                    count.incrementAndGet();

                    Assert.assertEquals(156L, buffer.getLong());
                    Assert.assertEquals(2, buffer.getInt());
                    return true;
                },
                () -> buffer
        );

        Assert.assertEquals(100_000, count.get());
    }
}