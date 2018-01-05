package com.fnklabs.dds.storage.im;

import com.fnklabs.dds.BytesUtils;
import com.fnklabs.dds.storage.ScanFunction;
import com.fnklabs.dds.storage.Storage;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

public class ImStorage implements Storage {
    private final ByteBuffer buffer;

    private final int allocatedSize;
    private final AtomicInteger actualSizeCounter = new AtomicInteger();
    private final AtomicInteger itemsCounter = new AtomicInteger();
    private int pageSize;

    public ImStorage(int maxSize) {
        buffer = ByteBuffer.allocateDirect(maxSize);
        buffer.order(ByteOrder.nativeOrder());

        this.allocatedSize = maxSize;
        this.pageSize = 4 * 1024;// 4 kb
    }

    public ImStorage(int allocatedSize, int pageSize) {
        this(allocatedSize);
        this.pageSize = pageSize;
    }

    @Override
    public int allocatedSize() {
        return allocatedSize;
    }

    @Override
    public int actualSize() {
        return actualSizeCounter.get();
    }

    @Override
    public int items() {
        return itemsCounter.get();
    }

    @Override
    public void write(int position, ByteBuffer data) {
        actualSizeCounter.addAndGet(data.limit());
        itemsCounter.incrementAndGet();

        try {
            ByteBuffer buffer = this.buffer.duplicate();

            buffer.position(position);
            buffer.put(data);
        } catch (Exception e) {
            LoggerFactory.getLogger(ImStorage.class).warn("can't write data to storage with position {}", position, e);
        }

    }

    @Override
    public void read(int position, ByteBuffer data) {

        ByteBuffer buffer = this.buffer.asReadOnlyBuffer();

        buffer.position(position);

        while (data.remaining() > 0) {
            data.put(buffer.get());
        }

    }

    @Override
    public void read(int position, byte[] dst) {
        ByteBuffer buffer = this.buffer.asReadOnlyBuffer();

        buffer.position(position);

        buffer.get(dst);
    }

    @Override
    public void scan(int position, ScanFunction scanFunction, Supplier<ByteBuffer> bufferSupplier) {
        ByteBuffer bufferCopy = this.buffer.duplicate();
        ByteBuffer blockBuffer = ByteBuffer.allocate(pageSize);
        ByteBuffer buffer = bufferSupplier.get();

        int maxPosition = actualSizeCounter.get();
        bufferCopy.position(position);
        int currentPosition = bufferCopy.position();

        while (bufferCopy.position() < maxPosition) {

            BytesUtils.read(bufferCopy, blockBuffer);

            blockBuffer.rewind();
            int blockIndex = 0;

            while (blockBuffer.remaining() > buffer.capacity()) { // read all from buffer

                BytesUtils.read(blockBuffer, buffer);

                buffer.rewind();

                if (!scanFunction.accept(currentPosition + blockIndex * buffer.capacity(), buffer)) {
                    return;
                }

                blockIndex++;
                buffer.clear();
            }

            currentPosition = currentPosition + blockBuffer.position();

            blockBuffer.compact();

        }
    }
}
