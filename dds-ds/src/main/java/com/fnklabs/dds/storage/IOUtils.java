package com.fnklabs.dds.storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

class IOUtils {
    static int read(FileChannel fileChannel, ByteBuffer buffer, long position) throws IOException {
        return fileChannel.read(buffer, position);

    }

    static int write(FileChannel fileChannel, ByteBuffer buffer, long position) throws IOException {
        try (FileLock fileLock = fileChannel.lock(position, buffer.limit(), false)) {
            return fileLock.channel().write(buffer, position);
        }
    }
}
