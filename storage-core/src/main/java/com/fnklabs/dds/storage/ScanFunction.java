package com.fnklabs.dds.storage;

@FunctionalInterface
public interface ScanFunction {

    boolean accept(long position, byte[] data);
}
