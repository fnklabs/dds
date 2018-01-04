package com.fnklabs.dds.storage.im;

import org.openjdk.jmh.annotations.*;

import java.nio.ByteBuffer;
import java.util.Random;
import java.util.concurrent.TimeUnit;

@Threads(value = 1)
@Fork(value = 1, jvmArgs = {
        "-server",
        "-Xms512m",
        "-Xmx4G",
        "-XX:NewSize=512m",
        "-XX:SurvivorRatio=6",
        "-XX:+AlwaysPreTouch",
        "-XX:+UseG1GC",
        "-XX:MaxGCPauseMillis=2000",
        "-XX:GCTimeRatio=4",
        "-XX:InitiatingHeapOccupancyPercent=30",
        "-XX:G1HeapRegionSize=8M",
        "-XX:ConcGCThreads=2",
        "-XX:G1HeapWastePercent=10",
        "-XX:+UseTLAB",
        "-XX:+ScavengeBeforeFullGC",
        "-XX:+DisableExplicitGC",
})
@Warmup(iterations = 5, timeUnit = TimeUnit.MILLISECONDS)
public class ImStorageBenchmarkTest {

    @Benchmark
    public void read(Context context, ReadParameters parameters) {
        context.imStorage.read(parameters.position, Integer.BYTES, parameters.buffer);
        parameters.buffer.rewind();
    }

    @Benchmark
    public void write(Context context, WriteParameters parameters) {
        context.imStorage.write(parameters.position, parameters.buffer);

        parameters.buffer.rewind();
    }


    @State(Scope.Benchmark)
    public static class Context {
        ImStorage imStorage;

        @Setup
        public void setUp() {
            imStorage = new ImStorage(64 * 1024 * 1024);
        }
    }


    @State(Scope.Thread)
    public static class ReadParameters {
        private static final Random RANDOM = new Random(64 * 1024 * 1024 - Integer.BYTES);

        int position;
        ByteBuffer buffer;

        @Setup
        public void setUp() {
            position = RANDOM.nextInt(64 * 1024 * 1024 - Integer.BYTES);
            buffer = ByteBuffer.allocate(Integer.BYTES);
        }
    }

    @State(Scope.Thread)
    public static class WriteParameters {
        private static final Random RANDOM = new Random(64 * 1024 * 1024 - Integer.BYTES);

        int position;
        ByteBuffer buffer;

        @Setup
        public void setUp() {
            position = RANDOM.nextInt(64 * 1024 * 1024 - Integer.BYTES);
            buffer = ByteBuffer.allocate(Integer.BYTES);
            buffer.putInt(1);
            buffer.rewind();
        }
    }

}