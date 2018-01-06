package com.fnklabs.buffer;

import java.nio.ByteBuffer;

class DirectBuffer extends AbstractByteBuffer {

    DirectBuffer(int size) {
        super(size, ByteBuffer.allocateDirect(size));
    }
}
