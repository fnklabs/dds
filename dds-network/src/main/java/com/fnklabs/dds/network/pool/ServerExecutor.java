package com.fnklabs.dds.network.pool;

import com.fnklabs.dds.network.ServerChannel;

import java.nio.channels.ClosedChannelException;
import java.nio.channels.SelectionKey;
import java.nio.channels.spi.AbstractSelectableChannel;
import java.util.function.Consumer;

public interface ServerExecutor extends NetworkExecutor<ServerChannel> {
    SelectionKey registerOpAccept(AbstractSelectableChannel channel, Consumer<SelectionKey> consumer) throws ClosedChannelException;

    SelectionKey registerOpRead(AbstractSelectableChannel channel, Consumer<SelectionKey> consumer) throws ClosedChannelException;

    SelectionKey registerOpWrite(AbstractSelectableChannel channel, Consumer<SelectionKey> consumer) throws ClosedChannelException;
}
