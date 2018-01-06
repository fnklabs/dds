package com.fnklabs.dds.storage.im;

import com.fnklabs.buffer.Buffer;
import com.fnklabs.buffer.BufferType;
import com.fnklabs.dds.BytesUtils;
import com.fnklabs.dds.storage.ScanFunction;
import com.fnklabs.dds.storage.Storage;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

public class ImStorage implements Storage {
    private final Buffer buffer;

    private final long allocatedSize;
    private int pageSize;

    private AtomicLong writtenData = new AtomicLong();

    public ImStorage(long maxSize) {
        buffer = BufferType.DIRECT.get(maxSize);

        this.allocatedSize = maxSize;
        this.pageSize = 4 * 1024;// 4 kb
    }

    public ImStorage(int allocatedSize, int pageSize) {
        this(allocatedSize);
        this.pageSize = pageSize;
    }

    @Override
    public long allocatedSize() {
        return allocatedSize;
    }

    @Override
    public long actualSize() {
        return allocatedSize;
    }

    @Override
    public long items() {
        return itemsCounter.get();
    }

    @Override
    public void write(long position, ByteBuffer data) {


        try {
            ByteBuffer buffer = this.buffer.duplicate();

            buffer.position((int) position);
            buffer.put(data);

            writtenData.addAndGet(data.capacity());
        } catch (Exception e) {
            LoggerFactory.getLogger(ImStorage.class).warn("can't write data to storage with position {}", position, e);
        }

    }

    @Override
    public void read(long position, ByteBuffer data) {

        ByteBuffer buffer = this.buffer.asReadOnlyBuffer();

        buffer.position((int) position);

        while (data.remaining() > 0) {
            data.put(buffer.get());
        }

    }

    @Override
    public void read(long position, byte[] dst) {
        ByteBuffer buffer = this.buffer.asReadOnlyBuffer();

        buffer.position((int) position);

        buffer.get(dst);
    }

    @Override
    public void scan(long position, ScanFunction scanFunction, Supplier<ByteBuffer> bufferSupplier) {
        ByteBuffer bufferCopy = this.buffer.duplicate();
        ByteBuffer blockBuffer = ByteBuffer.allocate(pageSize);
        ByteBuffer buffer = bufferSupplier.get();

        int maxPosition = actualSizeCounter.get();
        bufferCopy.position((int) position);
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
