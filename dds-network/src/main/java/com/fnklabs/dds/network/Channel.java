package com.fnklabs.dds.network;

import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.spi.AbstractSelectableChannel;

public interface Channel {
    /**
     * Register current channel in provided selector for operation
     *
     * @param selector
     */
    void register(Selector selector);

    /**
     * Process new selection key.
     * <p>
     * Ordering of incoming events must be realized on channel side
     *
     * @param selectionKey
     */
    void process(SelectionKey selectionKey);

    AbstractSelectableChannel channel();
}
