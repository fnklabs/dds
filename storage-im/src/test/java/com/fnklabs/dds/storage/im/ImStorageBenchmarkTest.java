package com.fnklabs.dds.storage.im;

import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.profile.*;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

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
@Warmup(iterations = 5, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, timeUnit = TimeUnit.MICROSECONDS)
public class ImStorageBenchmarkTest {

    @Benchmark
    public void read(Context context, ReadParameters parameters) {
        context.imStorage.read(parameters.position, parameters.buffer);
        parameters.buffer.rewind();
    }

    @Benchmark
    public void write(Context context, WriteParameters parameters) {
        context.imStorage.write(parameters.position, parameters.buffer);

        parameters.buffer.rewind();
    }

    @Benchmark
    public void scanBuffer(ScanContext context, ScanParameters scanParameters) {
        for (int i = 0; i < ScanContext.ALLOCATED_SIZE; i += Integer.BYTES) {
            context.imStorage.read(i, scanParameters.byteBuffer);
            scanParameters.byteBuffer.clear();
        }
    }

    @Benchmark
    public void scanByte(ScanContext context, ScanParameters scanParameters) {
        for (int i = 0; i < ScanContext.ALLOCATED_SIZE; i += Integer.BYTES) {
            context.imStorage.read(i, scanParameters.buffer);
        }
    }

    @Benchmark
    public void scanBigBuffer(ScanContext context, ScanParameters scanParameters) {
        for (int i = 0; i < ScanContext.ALLOCATED_SIZE; i += scanParameters.bigBuffer.length) {
            context.imStorage.read(i, scanParameters.bigBuffer);
        }
    }

    @Benchmark
    public void scanFull(ScanContext context, ScanParameters scanParameters) {
        context.imStorage.scan(
                0,
                (position, data) -> true,
                () -> scanParameters.byteBuffer
        );
    }


    @State(Scope.Benchmark)
    public static class Context {
        public static final int ALLOCATED_SIZE = 64 * 1024 * 1024;
        ImStorage imStorage;

        @Setup
        public void setUp() {
            imStorage = new ImStorage(ALLOCATED_SIZE);
        }
    }

    @State(Scope.Benchmark)
    public static class ScanContext {
        public static final long ALLOCATED_SIZE = 512 * 1024 * 1024; // 4GB
        public static final long OPERATIONS = ALLOCATED_SIZE / Integer.BYTES;
        ImStorage imStorage;

        private ByteBuffer byteBuffer = ByteBuffer.allocate(Integer.BYTES);

        @Setup
        public void setUp() {
            imStorage = new ImStorage(ALLOCATED_SIZE);

            for (int i = 0; i < ALLOCATED_SIZE; i += Integer.BYTES) {
                byteBuffer.putInt(i);
                byteBuffer.rewind();
                imStorage.write(i, byteBuffer);
                byteBuffer.clear();
            }

            System.err.println(String.format("Items: %d size: %d/%d", imStorage.items(), imStorage.actualSize(), imStorage.allocatedSize()));
        }

        @TearDown
        public void tearDown() {
            System.err.println(String.format("Items: %d size: %d/%d", imStorage.items(), imStorage.actualSize(), imStorage.allocatedSize()));
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

    @State(Scope.Thread)
    public static class ScanParameters {

        byte[] buffer;
        byte[] bigBuffer;
        ByteBuffer byteBuffer;

        @Setup
        public void setUp() {
            buffer = new byte[Integer.BYTES];
            bigBuffer = new byte[4 * 1024];
            byteBuffer = ByteBuffer.allocate(Integer.BYTES);
        }
    }

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

        for (RunResult runResult : new Runner(opt).run()) {

        }
    }
}