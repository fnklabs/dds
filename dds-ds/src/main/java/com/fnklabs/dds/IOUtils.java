package com.fnklabs.dds;

import com.fnklabs.metrics.MetricsFactory;
import com.fnklabs.metrics.Timer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

public class IOUtils {
    public static ByteBuffer allocate(int capacity) {
        return ByteBuffer.allocate(capacity);
    }

    public static int read(FileChannel fileChannel, ByteBuffer buffer, long position) throws IOException {
        try (
                Timer timer = MetricsFactory.getMetrics().getTimer("dds.io.utils.read");
                FileLock fileLock = fileChannel.lock(position, buffer.remaining(), true)
        ) {
            return fileLock.channel().read(buffer, position);
        }
    }

    public static int write(FileChannel fileChannel, ByteBuffer buffer, long position) throws IOException {
        try (
                Timer timer = MetricsFactory.getMetrics().getTimer("dds.io.utils.write");
                FileLock fileLock = fileChannel.lock(position, buffer.remaining(), false)
        ) {
            return fileLock.channel().write(buffer, position);
        }
    }
}
