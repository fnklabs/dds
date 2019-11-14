package com.fnklabs.dds.network;

import java.nio.channels.SocketChannel;

public class ChannelClosedException extends Exception {
    private final SocketChannel socketChannel;

    public ChannelClosedException(SocketChannel socketChannel) {this.socketChannel = socketChannel;}

    public SocketChannel getSocketChannel() {
        return socketChannel;
    }
}
