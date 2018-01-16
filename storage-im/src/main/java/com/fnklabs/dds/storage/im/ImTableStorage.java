package com.fnklabs.dds.storage.im;

import com.fnklabs.buffer.Buffer;
import com.fnklabs.buffer.BufferType;
import com.fnklabs.dds.storage.ScanFunction;
import com.fnklabs.dds.storage.TableStorage;
import com.google.common.base.Verify;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

public class ImTableStorage implements TableStorage {
    private final int bufferSize;
    private final long maxSize;

    private final Buffer buffer;

    private final AtomicLong lastPosition = new AtomicLong();

    private final ThreadLocal<byte[]> storageBuffer;


    ImTableStorage(long maxSize, int bufferSize) {

        this.maxSize = maxSize;
        this.bufferSize = bufferSize;


        this.buffer = BufferType.DIRECT.get(maxSize);

        this.storageBuffer = ThreadLocal.withInitial(() -> new byte[bufferSize]);
    }

    @Override
    public long allocatedSize() {
        return buffer.bufferSize();
    }

    @Override
    public void write(long position, byte[] data) {
        buffer.write(position, data);

        long currentValue = lastPosition.get();

        if (currentValue < position + data.length) {
            lastPosition.compareAndSet(currentValue, position + data.length);
        }

    }

    @Override
    public int read(long position, byte[] buffer) {
        int availableBufferLength = (int) (maxSize - position); // available size for read in current buffer

        int length = buffer.length < availableBufferLength ? buffer.length : availableBufferLength;

        this.buffer.read(position, buffer, 0, length);

        return length;
    }


    @Override
    public void scan(long position, long end, ScanFunction scanFunction, Supplier<byte[]> bufferSupplier) {
        Verify.verify(position < end, "end position must be > start position");

        byte[] storageBuffer = this.storageBuffer.get();
        byte[] dataBuffer = bufferSupplier.get();

        Verify.verify(storageBuffer.length >= dataBuffer.length, "buffer can't be bigger than storage buffer");

        long positionOffset = position;

        while (positionOffset < end) {
            int readBytes = read(positionOffset, storageBuffer);

            if (end - positionOffset < readBytes) {
                readBytes = (int) (end - positionOffset);
            }

            for (int i = 0; i < readBytes; i += dataBuffer.length) {
                System.arraycopy(storageBuffer, i, dataBuffer, 0, dataBuffer.length);

                if (!scanFunction.accept(positionOffset + i, dataBuffer)) {
                    return;
                }
            }

            int tail = readBytes % dataBuffer.length;

            positionOffset += readBytes - tail;
        }
    }
}
