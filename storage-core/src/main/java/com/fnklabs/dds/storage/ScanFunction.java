package com.fnklabs.dds.storage;

import java.nio.ByteBuffer;

@FunctionalInterface
public interface ScanFunction {

    boolean accept(long position, ByteBuffer data);
}
