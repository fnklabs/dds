package com.fnklabs.dds.storage.im;

import com.fnklabs.buffer.Buffer;
import com.fnklabs.buffer.BufferType;
import com.fnklabs.dds.storage.ScanFunction;
import com.fnklabs.dds.storage.TableStorage;
import com.google.common.base.Verify;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

public class ImTableStorage implements TableStorage {
    private final int buffeSize;
    private final long maxSize;
    private final int chunkSize;

    private final Map<Integer, Buffer> buffers = new ConcurrentHashMap<>();

    private final AtomicLong lastPosition = new AtomicLong();

    public ImTableStorage(long maxSize) {
        this(maxSize, 4 * 1024, 512 * 1024 * 1024);
    }

    public ImTableStorage(long maxSize, int buffeSize, int chunkSize) {
        this.maxSize = maxSize;
        this.buffeSize = buffeSize;
        this.chunkSize = chunkSize;
    }

    private Buffer getBuffer(long position) {
        int bufferIndex = (int) (position / chunkSize);

        Verify.verify(bufferIndex * chunkSize < maxSize);

        return buffers.computeIfAbsent(bufferIndex, (k) -> {
            return BufferType.DIRECT.get(chunkSize);
        });
    }

    @Override
    public long allocatedSize() {
        return buffers.size() * chunkSize;
    }

    @Override
    public void read(long position, byte[] data) {
        int dataOffset = 0;

        while (dataOffset < data.length) {
            Buffer buff = getBuffer(position + dataOffset);

            int buffOffset = (int) (position % chunkSize);

            int availableSize = (int) (buff.bufferSize() - buffOffset);

            int dataToCopy = data.length < availableSize ? data.length : availableSize;

            buff.read(buffOffset, data, dataOffset, dataToCopy);

            dataOffset += dataToCopy;
        }
    }

    @Override
    public void write(long position, byte[] data) {
        int dataOffset = 0;

        while (dataOffset < data.length) {
            Buffer buff = getBuffer(position + dataOffset);

            int buffOffset = (int) (position % chunkSize);

            int availableSize = (int) (buff.bufferSize() - buffOffset);

            int dataToCopy = data.length < availableSize ? data.length : availableSize;

            buff.write(buffOffset, data, dataOffset, dataToCopy);

            dataOffset += dataToCopy;
        }

        lastPosition.addAndGet(position + data.length);
    }

    @Override
    public void scan(long position, ScanFunction scanFunction, Supplier<byte[]> bufferSupplier) {
        byte[] buffer = bufferSupplier.get();

        long positionOffset = 0;

        while (positionOffset < lastPosition.get()) {
            read(positionOffset, buffer);

            if (scanFunction.accept(positionOffset, buffer)) {
                positionOffset += buffer.length;
                continue;
            }

            break;
        }
    }
}
