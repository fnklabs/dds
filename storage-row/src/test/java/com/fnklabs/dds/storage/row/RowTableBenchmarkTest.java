package com.fnklabs.dds.storage.row;

import com.fnklabs.dds.storage.Record;
import com.fnklabs.dds.storage.column.Column;
import com.fnklabs.dds.storage.column.IntegerColumn;
import com.fnklabs.dds.storage.column.LongColumn;
import com.fnklabs.dds.storage.im.ImStorageFactory;
import com.fnklabs.dds.storage.query.Condition;
import org.openjdk.jmh.annotations.*;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

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
        "-XX:InitiatingHeapOccupancyPercent=30",
        "-XX:G1HeapRegionSize=8M",
        "-XX:ConcGCThreads=2",
        "-XX:G1HeapWastePercent=10",
        "-XX:+UseTLAB",
        "-XX:+ScavengeBeforeFullGC",
        "-XX:+DisableExplicitGC",
})
@Warmup(iterations = 10, timeUnit = TimeUnit.MILLISECONDS)
public class RowTableBenchmarkTest {

    @Benchmark
    public void write(WriteContext context) {
        context.table.write(context.next());
    }

    @Benchmark
    public void read(ReadContext readContext) {
        readContext.table.read(readContext.next());
    }

    @Benchmark
    public void query(QueryContext context) {
        context.table.query("price", context.condition, new PriceTotal.SumPrice());
    }

    @State(Scope.Benchmark)
    public static class ReadContext {
        private static final AtomicLong counter = new AtomicLong();

        RowTable table;
        private LongColumn idColumn;
        private LongColumn createdAtColumn;
        private IntegerColumn priceColumn;

        @Setup
        public void setUp() {
            idColumn = new LongColumn("id", (short) 0, true);
            createdAtColumn = new LongColumn("created_at", (short) 1);
            priceColumn = new IntegerColumn("price", (short) 2);

            table = new RowTable(
                    "test",
                    Arrays.asList(idColumn, createdAtColumn, priceColumn),
                    256 * 1024 * 1024,
                    new ImStorageFactory()
            );

            for (long i = 0; i < 4; i++) {
                table.write(next(i));
            }
        }

        byte[] next() {
            ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);

            buffer.putLong(counter.incrementAndGet());

            return buffer.array();
        }

        private Record next(long i) {
            return new Record(
                    new HashMap<Column, Object>() {{
                        put(idColumn, i);
                        put(createdAtColumn, 1L);
                        put(priceColumn, 2);
                    }}
            );
        }
    }

    @State(Scope.Benchmark)
    public static class WriteContext {
        private static final AtomicLong counter = new AtomicLong();
        RowTable table;
        private LongColumn idColumn;
        private LongColumn createdAtColumn;
        private IntegerColumn priceColumn;

        @Setup
        public void setUp() {
            idColumn = new LongColumn("id", (short) 0, true);
            createdAtColumn = new LongColumn("created_at", (short) 1);
            priceColumn = new IntegerColumn("price", (short) 2);

            table = new RowTable(
                    "test",
                    Arrays.asList(idColumn, createdAtColumn, priceColumn),
                    256 * 1024 * 1024,
                    new ImStorageFactory()
            );
        }

        Record next() {
            return new Record(
                    new HashMap<Column, Object>() {{
                        put(idColumn, counter.incrementAndGet());
                        put(createdAtColumn, 1L);
                        put(priceColumn, 2);
                    }}
            );
        }
    }

    @State(Scope.Benchmark)
    public static class QueryContext {
        RowTable table;
        Condition<Long> condition;

        private LongColumn idColumn;
        private LongColumn createdAtColumn;
        private IntegerColumn priceColumn;

        @Setup
        public void setUp() {
            idColumn = new LongColumn("id", (short) 0, true);
            createdAtColumn = new LongColumn("created_at", (short) 1);
            priceColumn = new IntegerColumn("price", (short) 2);

            table = new RowTable(
                    "test",
                    Arrays.asList(idColumn, createdAtColumn, priceColumn),
                    256 * 1024 * 1024,
                    new ImStorageFactory()
            );

            for (long i = 0; i < 100_000; i++) {
                table.write(next(i));
            }

            condition = new Condition<>((t) -> t.equals(1L));
        }

        private Record next(long i) {
            long id = i % 100;

            return new Record(
                    new HashMap<Column, Object>() {{
                        put(idColumn, id);
                        put(createdAtColumn, 1L);
                        put(priceColumn, 1);
                    }}
            );
        }
    }

}