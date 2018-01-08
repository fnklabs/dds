package com.fnklabs.dds.storage.im;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.profile.*;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.Random;
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
public class ImStorageBenchmarkTest {

    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(ImStorageBenchmarkTest.class.getName())
                .addProfiler(GCProfiler.class)
                .addProfiler(HotspotMemoryProfiler.class)
                .addProfiler(HotspotRuntimeProfiler.class)
                .addProfiler(HotspotThreadProfiler.class)
                .addProfiler(PausesProfiler.class)
                .addProfiler(StackProfiler.class)
                .verbosity(VerboseMode.EXTRA)
                .build();
    }

    @Benchmark
    public void read(Context context, ReadParameters parameters) {
        context.imStorage.read(parameters.position, parameters.buffer);
    }

    @Benchmark
    public void readBuffer(Context context, ReadParameters parameters) {
        context.imStorage.read(parameters.position, parameters.buffer);
    }

    @Benchmark
    public void write(Context context, WriteParameters parameters) {
        context.imStorage.write(parameters.position, parameters.buffer);
    }

    @Benchmark
    public void scan(ScanContext context, ScanParameters parameters) {
        context.imStorage.scan(
                0,
                (position, data) -> true,
                () -> parameters.buffer
        );
    }

    @State(Scope.Benchmark)
    public static class Context {
        public static final int ALLOCATED_SIZE = 512 * 1024 * 1024;
        ImTableStorage imStorage;

        @Setup
        public void setUp() {
            imStorage = new ImTableStorage(ALLOCATED_SIZE);
        }
    }

    @State(Scope.Benchmark)
    public static class ScanContext extends Context {

        @Setup
        public void setUp() {
            super.setUp();

            imStorage.write(ALLOCATED_SIZE - 2, new byte[1]);
        }
    }

    @State(Scope.Thread)
    public static class ReadParameters {
        private static final Random RANDOM = new Random(64 * 1024 * 1024 - Integer.BYTES);

        int position;
        byte[] buffer;

        @Setup
        public void setUp() {
            position = RANDOM.nextInt(64 * 1024 * 1024 - Integer.BYTES);
            buffer = new byte[4];
        }
    }

    @State(Scope.Thread)
    public static class WriteParameters {
        private static final Random RANDOM = new Random(64 * 1024 * 1024 - Integer.BYTES);

        int position;
        byte[] buffer;

        @Setup
        public void setUp() {
            position = RANDOM.nextInt(64 * 1024 * 1024 - Integer.BYTES);
            buffer = new byte[4];
        }
    }

    @State(Scope.Thread)
    public static class ScanParameters {

        byte[] buffer;

        @Setup
        public void setUp() {
            buffer = new byte[64 * 1024];
        }
    }
}