package com.fnklabs.buffer;

import java.nio.ByteBuffer;

class HeapBuffer extends AbstractByteBuffer {

    HeapBuffer(int size) {
        super(size, ByteBuffer.allocateDirect(size));
    }
}
