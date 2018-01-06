package com.fnklabs.buffer;

import com.google.common.base.Verify;

import java.util.function.Function;

public enum BufferType {
    HEAP((size) -> {
        Verify.verify(size <= Integer.MAX_VALUE, "size can't be higher that %d bytes", Integer.MAX_VALUE);

        return new HeapBuffer(size.intValue());
    }),
    DIRECT((size) -> {
        Verify.verify(size <= Integer.MAX_VALUE, "size can't be higher that %d bytes", Integer.MAX_VALUE);

        return new DirectBuffer(size.intValue());
    }),;
    private final Function<Long, Buffer> bufferSupplier;

    BufferType(Function<Long, Buffer> bufferSupplier) {this.bufferSupplier = bufferSupplier;}

    public Buffer get(long size) {
        return bufferSupplier.apply(size);
    }
}
