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

    AbstractByteBuffer(int size, ByteBuffer buffer) {
        buffer.order(ByteOrder.nativeOrder());

        this.size = size;
        this.buffer = buffer;
    }

    @Override
    public long bufferSize() {
        return size;
    }

    @Override
    public void read(long position, byte[] data) {
        Verify.verify(position < size, "position can't be higher that size %d", size);

        ByteBuffer buffer = this.buffer.duplicate();
        buffer.position((int) position);

        int length = buffer.remaining() > data.length ? data.length : buffer.remaining();

        buffer.get(data, 0, length);
    }

    @Override
    public void read(long position, byte[] dstBuffer, int offset, int length) {
        Verify.verify(position < size, "position can't be higher that size %d", size);

        ByteBuffer buffer = this.buffer.duplicate();

        Verify.verify(buffer.position() + length < buffer.limit());
        Verify.verify(
                dstBuffer.length - offset >= length,
                "invalid offset parameters for read. offset: %s length: %s data.length = %s",
                offset,
                length,
                dstBuffer.length
        );

        buffer.position((int) position);
        buffer.get(dstBuffer, (int) offset, length);
    }

    @Override
    public void write(long position, byte[] data) {
        Verify.verify(position < size, "position %s can't be higher that size: %ы", position, size);

        ByteBuffer duplicate = buffer.duplicate();

        duplicate.position((int) position);
        duplicate.put(data);

        allocatedSize.addAndGet(data.length);
        itemsCount.incrementAndGet();
    }

    @Override
    public void write(long position, byte[] data, int offset, int length) {
        Verify.verify(position < size, "position %s can't be higher that size: %ы", position, size);

        ByteBuffer duplicate = buffer.duplicate();

        duplicate.position((int) position);
        duplicate.put(data, (int) offset, length);

        allocatedSize.addAndGet(length);
        itemsCount.incrementAndGet();
    }
}
