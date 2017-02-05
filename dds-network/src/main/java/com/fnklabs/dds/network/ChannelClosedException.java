package com.fnklabs.dds.network;

import lombok.Getter;
import lombok.RequiredArgsConstructor;

import java.nio.channels.SocketChannel;

@RequiredArgsConstructor
@Getter
public class ChannelClosedException extends Exception {
    private final SocketChannel socketChannel;
}
