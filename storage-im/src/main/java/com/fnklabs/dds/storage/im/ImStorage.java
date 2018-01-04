package com.fnklabs.dds.storage.im;

import com.fnklabs.dds.storage.Storage;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.locks.ReentrantLock;

public class ImStorage implements Storage {
    private final ByteBuffer buffer;

    private final ReentrantLock reentrantLock = new ReentrantLock();

    public ImStorage(int maxSize) {
        buffer = ByteBuffer.allocateDirect(maxSize);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
    }

    @Override
    public void write(int position, ByteBuffer data) {
        reentrantLock.lock();

        try {
            buffer.position(position);
            buffer.put(data);
        } catch (Exception e) {
            LoggerFactory.getLogger(ImStorage.class).warn("can't write data to storage with position {}", position, e);
        } finally {
            reentrantLock.unlock();
        }

    }

    @Override
    public void read(int position, int length, ByteBuffer data) {
        reentrantLock.lock();

        buffer.position(position);

        for (int i = 0; i < length; i++) {
            byte b = buffer.get();
            data.put(b);
        }

        reentrantLock.unlock();
    }
}
