package com.fnklabs.dds.network;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;


public class ByteBufferTest {
    private final static Logger log = LoggerFactory.getLogger(ByteBufferTest.class);

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

        buffer.put((byte) 4);

        log.debug("Buffer: {}/{}", buffer.array(), buffer);
    }

}
