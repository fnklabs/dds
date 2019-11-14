package com.fnklabs.buffer;

import com.alexkasko.unsafe.offheap.OffHeapMemory;

import java.io.Closeable;
import java.io.IOException;

class UnsafeBuffer implements Buffer, Closeable {
    private final long size;

    private final OffHeapMemory offHeapMemory;

    public UnsafeBuffer(long size) {
        this.size = size;

        try {
            offHeapMemory = OffHeapMemory.allocateMemoryUnsafe(size);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public long bufferSize() {
        return size;
    }

    @Override
    public void read(long position, byte[] data) {
        offHeapMemory.get(position, data);
    }

    @Override
    public void read(long position, byte[] data, int offset, int length) {
        offHeapMemory.get(position, data, (int) offset, length);
    }

    @Override
    public void write(long position, byte[] data) {
        offHeapMemory.put(position, data);
    }

    @Override
    public void write(long position, byte[] data, int offset, int length) {
        offHeapMemory.put(position, data, offset, length);

    }

    @Override
    public void close() throws IOException {
        offHeapMemory.free();
    }


}
