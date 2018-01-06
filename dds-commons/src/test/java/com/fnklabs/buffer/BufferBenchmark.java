package com.fnklabs.buffer;

import org.junit.Before;
import org.openjdk.jmh.annotations.*;

import java.util.concurrent.TimeUnit;

@Threads(value = 4)
@Fork(value = 4, jvmArgs = {
        "-server",
        "-Xms512m",
        "-Xmx4G",
        "-XX:NewSize=512m",
        "-XX:SurvivorRatio=6",
        "-XX:+AlwaysPreTouch",
        "-XX:+UseG1GC",
        "-XX:MaxGCPauseMillis=2000",
        "-XX:GCTimeRatio=4",
        "-XX:InitiatingHeapOccupancyPercent=70",
        "-XX:G1HeapRegionSize=8M",
        "-XX:ConcGCThreads=2",
        "-XX:G1HeapWastePercent=10",
        "-XX:+UseTLAB",
        "-XX:+ScavengeBeforeFullGC",
        "-XX:+DisableExplicitGC",
        "-XX:MaxDirectMemorySize=1G",
        "-XX:+UseLargePages",
        "-XX:+UseCompressedOops"
})
@Warmup(iterations = 20, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 20, timeUnit = TimeUnit.MICROSECONDS)
public class BufferBenchmark {


    @Benchmark
    public void readDirect(DirectContext context) {
        context.buffer.read(context.dataBuffer);
    }

    @Benchmark
    public void writeDirect(DirectContext context) {
        context.buffer.write(context.dataBuffer);
    }

    @Benchmark
    public void readHeap(HeapContext context) {
        context.buffer.read(context.dataBuffer);
    }

    @Benchmark
    public void writeHeap(HeapContext context) {
        context.buffer.write(context.dataBuffer);
    }


    @State(Scope.Benchmark)
    public static class DirectContext extends Context {

        @Setup
        public void setUp() {
            super.setUp();
            buffer = Type.DIRECT.get(ALLOCATED_SIZE);
        }
    }

    @State(Scope.Benchmark)
    public static class HeapContext extends Context {

        @Setup
        public void setUp() {
            super.setUp();
            buffer = Type.HEAP.get(ALLOCATED_SIZE);
        }
    }

    @State(Scope.Benchmark)
    public static abstract class Context {
        public static final int ALLOCATED_SIZE = 512 * 1024 * 1024;

        @Param({"4", "8", "20", "32", "64", "4096", "65536"})
        int bufferSize;

        Buffer buffer;

        byte[] dataBuffer;

        @Before
        public void setUp() {
            dataBuffer = new byte[bufferSize];
        }

    }


}