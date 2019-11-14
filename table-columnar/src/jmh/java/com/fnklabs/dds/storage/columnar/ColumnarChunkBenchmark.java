package com.fnklabs.dds.storage.columnar;

import com.fnklabs.dds.storage.TableStorage;
import com.fnklabs.dds.storage.im.ImStorageFactory;
import com.fnklabs.dds.storage.im.ImStorageOptions;
import com.fnklabs.dds.table.*;
import com.fnklabs.dds.table.codec.CodecRegistry;
import com.google.common.collect.Range;
import com.google.protobuf.ByteString;
import org.junit.After;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.profile.*;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.nio.ByteBuffer;
import java.util.concurrent.TimeUnit;

@Threads(value = 4)
@Fork(value = 4, jvmArgs = {
        "-server",
        "-Xms512m",
        "-Xmx6G",
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
@Measurement(iterations = 5, timeUnit = TimeUnit.MICROSECONDS)
public class ColumnarChunkBenchmark {
    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(ColumnarChunkBenchmark.class.getName())
                .addProfiler(GCProfiler.class)
                .addProfiler(HotspotMemoryProfiler.class)
                .addProfiler(HotspotRuntimeProfiler.class)
                .addProfiler(HotspotThreadProfiler.class)
                .addProfiler(PausesProfiler.class)
                .addProfiler(StackProfiler.class)
                .verbosity(VerboseMode.EXTRA)
                .build();

        new Runner(opt).run();
    }

    @Benchmark
    public void insertQuery(InsertContext context) {
        context.chunk.query(context.insertQuery);
    }

    @Benchmark
    public void selectWithoutCondition(ScanContext context) {
        context.chunk.query(context.select);
    }

    @Benchmark
    public void selectWithCondition(SelectContext context) {
        context.chunk.query(context.select);
    }

    @After
    public void tearDown() throws Exception {

    }

    @State(Scope.Benchmark)
    public abstract static class Context {
        public static final int ALLOCATED_SIZE = 512 * 1024 * 1024;
        protected ByteBuffer idBuffer;
        protected ByteBuffer field1Buffer;
        protected ByteBuffer field2Buffer;
        protected ByteBuffer field3Buffer;
        ColumnarChunk chunk;
        TableDefinition tableDefinition;
        ColumnDefinition idField;
        ColumnDefinition field1;
        ColumnDefinition field2;
        ColumnDefinition field3;

        @Setup
        public void setUp() {
            idField = ColumnDefinition.newBuilder()
                                      .setName("id")
                                      .setDataType(DataType.LONG)
                                      .setPrimary(true)
                                      .setSize(Long.BYTES)
                                      .build();
            field1 = ColumnDefinition.newBuilder()
                                     .setName("field-1")
                                     .setDataType(DataType.LONG)
                                     .setSize(Long.BYTES)
                                     .build();
            field2 = ColumnDefinition.newBuilder()
                                     .setName("field-2")
                                     .setDataType(DataType.LONG)
                                     .setSize(Long.BYTES)
                                     .build();

            field3 = ColumnDefinition.newBuilder()
                                     .setName("field-3")
                                     .setDataType(DataType.LONG)
                                     .setSize(Long.BYTES)
                                     .build();

            tableDefinition = TableDefinition.newBuilder()
                                             .setName("test")
                                             .addColumn(idField)
                                             .addColumn(field1)
                                             .addColumn(field2)
                                             .addColumn(field3)
                                             .build();

            TableStorage storage = new ImStorageFactory().get(
                    new ImStorageOptions(ALLOCATED_SIZE, 4 * 1024)
            );

            chunk = new ColumnarChunk(0, ALLOCATED_SIZE, tableDefinition, storage, Range.<Long>closedOpen(Long.MIN_VALUE, Long.MAX_VALUE));

            idBuffer = ByteBuffer.allocate(idField.getSize());
            field1Buffer = ByteBuffer.allocate(field1.getSize());
            field2Buffer = ByteBuffer.allocate(field2.getSize());
            field3Buffer = ByteBuffer.allocate(field3.getSize());

        }
    }

    @State(Scope.Benchmark)
    public static class ScanContext extends Context {
        Select select;

        @Setup
        public void setUp() {
            super.setUp();

            CodecRegistry.get(DataType.LONG).encode(1L, idBuffer);
            CodecRegistry.get(DataType.LONG).encode(2L, field1Buffer);
            CodecRegistry.get(DataType.LONG).encode(3L, field2Buffer);
            CodecRegistry.get(DataType.LONG).encode(4L, field3Buffer);

            int rowSize = tableDefinition.getColumnCount() * Long.BYTES;

            for (int i = 0; i < Context.ALLOCATED_SIZE / rowSize - 1; i++) {
                Insert insert = Insert.newBuilder()
                                      .setTable("test")
                                      .putValue("id", ByteString.copyFrom(idBuffer.array()))
                                      .putValue("field-1", ByteString.copyFrom(field1Buffer.array()))
                                      .putValue("field-2", ByteString.copyFrom(field2Buffer.array()))
                                      .putValue("field-3", ByteString.copyFrom(field3Buffer.array()))
                                      .build();


                ResultSet query = chunk.query(insert);

                if (query.getWasApplied()) {
                    // do something
                }
            }

            select = Select.newBuilder()
                           .addSelection(Selection.newBuilder().setColumn("id").setAggregateFunction(AggregationFunction.COUNT).build())
                           .setTable("test")
                           .build();
        }


    }

    @State(Scope.Benchmark)
    public static class SelectContext extends ScanContext {
        Select select;

        @Setup
        public void setUp() {
            super.setUp();


            select = Select.newBuilder()
                           .addSelection(Selection.newBuilder().setColumn("id").setAggregateFunction(AggregationFunction.COUNT).build())
                           .setTable("test")
                           .setWhere(
                                   Where.newBuilder()
                                        .addClauses(
                                                Clause.newBuilder()
                                                      .setColumn("id")
                                                      .setExpression(Expression.EQ)
                                                      .setValue(ByteString.copyFrom(idBuffer.array())).build()
                                        )
                                        .build()
                           )
                           .build();
        }
    }

    @State(Scope.Thread)
    public static class InsertContext extends Context {
        Insert insertQuery;

        @Setup
        public void setUp() {
            super.setUp();

            CodecRegistry.get(DataType.LONG).encode(1L, idBuffer);
            CodecRegistry.get(DataType.LONG).encode(2L, field1Buffer);
            CodecRegistry.get(DataType.LONG).encode(3L, field2Buffer);
            CodecRegistry.get(DataType.LONG).encode(4L, field3Buffer);

            insertQuery = Insert.newBuilder()
                                .setTable("test")
                                .putValue("id", ByteString.copyFrom(idBuffer.array()))
                                .putValue("field-1", ByteString.copyFrom(field1Buffer.array()))
                                .putValue("field-2", ByteString.copyFrom(field2Buffer.array()))
                                .putValue("field-3", ByteString.copyFrom(field3Buffer.array()))
                                .build();
        }
    }
}
