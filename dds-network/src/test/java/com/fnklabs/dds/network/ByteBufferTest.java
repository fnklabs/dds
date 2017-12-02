package com.fnklabs.dds.network;

import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.nio.ByteBuffer;

@Slf4j
public class ByteBufferTest {
    @Test
    public void compact() throws Exception {
        ByteBuffer buffer = ByteBuffer.allocate(3);
        buffer.put((byte) 1);
        buffer.put((byte) 2);
        buffer.put((byte) 3);

        log.debug("Buffer: {}/{}", buffer.array(), buffer);

        buffer.flip();

        log.debug("Buffer: {}/{}", buffer.array(), buffer);

        buffer.get();

        log.debug("Buffer: {}/{}", buffer.array(), buffer);

        buffer.compact();

        log.debug("Buffer: {}/{}", buffer.array(), buffer);

        buffer.put((byte)4);

        log.debug("Buffer: {}/{}", buffer.array(), buffer);
    }

}
