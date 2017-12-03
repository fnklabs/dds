package com.fnklabs.dds.network.pool;

import java.io.Closeable;
import java.io.IOException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.spi.AbstractSelectableChannel;
import java.util.function.Consumer;

/**
 * Network pool for
 */
public interface NetworkExecutor extends Closeable {
    SelectionKey registerOpAccept(AbstractSelectableChannel channel, Consumer<SelectionKey> consumer) throws ClosedChannelException;

    SelectionKey registerOpRead(AbstractSelectableChannel channel, Consumer<SelectionKey> consumer) throws ClosedChannelException;

    SelectionKey registerOpWrite(AbstractSelectableChannel channel, Consumer<SelectionKey> consumer) throws ClosedChannelException;

    void run();

    void shutdown();

    @Override
    default void close() throws IOException {
        shutdown();
    }
}
