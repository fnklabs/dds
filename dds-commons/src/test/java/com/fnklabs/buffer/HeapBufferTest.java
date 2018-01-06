package com.fnklabs.buffer;

import org.junit.Before;
import org.junit.Test;

public class HeapBufferTest {
    public HeapBuffer buffer;

    private byte[] dataBuffer;

    @Before
    public void setUp() throws Exception {
        buffer = new HeapBuffer(32 * 1024);
        dataBuffer = new byte[32];
    }

    @Test
    public void read() {
        int read = buffer.read(dataBuffer);
        buffer.read(dataBuffer);
    }

    @Test
    public void write() {
        buffer.write(dataBuffer);
    }
}