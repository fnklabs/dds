package com.fnklabs.buffer;

import com.google.common.base.Verify;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.atomic.AtomicLong;

abstract class AbstractByteBuffer implements Buffer {
    private final long size;

    private final ByteBuffer buffer;

    private final AtomicLong itemsCount = new AtomicLong();
    private final AtomicLong allocatedSize = new AtomicLong(0);

    public AbstractByteBuffer(int size, ByteBuffer buffer) {
        buffer.order(ByteOrder.nativeOrder());

        this.size = size;
        this.buffer = buffer;
    }

    @Override
    public long allocatedSize() {
        return allocatedSize.get();
    }

    @Override
    public long bufferSize() {
        return size;
    }

    @Override
    public int read(long position, byte[] data) {
        Verify.verify(position < size, "position can't be higher that size %d", size);

        ByteBuffer buffer = this.buffer.duplicate();
        buffer.position((int) position);
        buffer.get(data);

        return buffer.position() - (int) position;
    }

    @Override
    public int read(byte[] data) {
        ByteBuffer buffer = this.buffer.duplicate();

        int currentPosition = buffer.position();
        buffer.get(data);

        return buffer.position() - currentPosition;
    }

    @Override
    public void write(long position, byte[] data) {
        Verify.verify(position < size, "position can't be higher that size: %d", size);

        ByteBuffer duplicate = buffer.duplicate();

        duplicate.position((int) position);
        duplicate.put(data);

        allocatedSize.addAndGet(data.length);
        itemsCount.incrementAndGet();
    }

    @Override
    public void write(byte[] data) {
        ByteBuffer duplicate = buffer.duplicate();

        duplicate.put(data);

        allocatedSize.addAndGet(data.length);
        itemsCount.incrementAndGet();
    }
}
