package com.fnklabs.buffer;

import com.google.common.base.Verify;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.concurrent.TimeUnit;

@Threads(value = 8)
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
@Warmup(iterations = 10, timeUnit = TimeUnit.MILLISECONDS)
@Measurement(iterations = 10, timeUnit = TimeUnit.MICROSECONDS)
public class BufferBenchmark {
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(BufferBenchmark.class.getName())
                .verbosity(VerboseMode.EXTRA)
                .build();

        new Runner(opt).run();
    }

    @Benchmark
    public void read(Context context) {
        Verify.verifyNotNull(context.buffer);
        Verify.verifyNotNull(context.dataBuffer);

        context.buffer.read(0, context.dataBuffer);
    }


    @Benchmark
    public void write(Context context) {
        context.buffer.write(0, context.dataBuffer);
    }


    @Benchmark
    public void scan(Context context) {
        for (int i = 0; i < context.buffer.bufferSize(); i += context.bufferSize) {
            context.buffer.read(0, context.dataBuffer);
        }
    }


    @State(Scope.Benchmark)
    public static class Context {
        public static final int ALLOCATED_SIZE = 512 * 1024 * 1024;

        @Param({
//                "8",
                "64",
                "4096",
//                "65536"
        })
        int bufferSize;

        @Param({"HEAP", "DIRECT", "UNSAFE"})
        BufferType bufferType;

        Buffer buffer;


        byte[] dataBuffer;

        @Setup
        public void setUp() {
            dataBuffer = new byte[bufferSize];
            buffer = bufferType.get(ALLOCATED_SIZE);
        }

    }


}