package com.fnklabs.dds.storage.columnar;

import com.fnklabs.dds.storage.TableStorage;
import com.fnklabs.dds.storage.im.ImStorageFactory;
import com.fnklabs.dds.storage.im.ImStorageOptions;
import com.fnklabs.dds.table.ColumnDefinition;
import com.fnklabs.dds.table.DataType;
import com.fnklabs.dds.table.TableDefinition;
import com.google.common.collect.Range;
import org.junit.Before;

import java.nio.ByteBuffer;

public class AbstractChunkTest {
    protected ColumnarChunk chunk;
    protected ColumnDefinition field2;
    protected ColumnDefinition field1;
    protected ColumnDefinition idField;

    protected ByteBuffer idBuffer;
    protected ByteBuffer field1Buffer;
    protected ByteBuffer field2Buffer;

    @Before
    public void setUp() throws Exception {
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
                                 .setDataType(DataType.STRING)
                                 .setSize(64)
                                 .build();

        TableDefinition tableDefinition = TableDefinition.newBuilder()
                                                         .setName("test")
                                                         .addColumn(idField)
                                                         .addColumn(field1)
                                                         .addColumn(field2)
                                                         .build();
        TableStorage storage = new ImStorageFactory().get(
                new ImStorageOptions(512 * 1024 * 1024, 4 * 1024)
        );

        chunk = new ColumnarChunk(0, 64 * 1024 * 1024, tableDefinition, storage, Range.<Long>closedOpen(Long.MIN_VALUE, Long.MAX_VALUE));

        idBuffer = ByteBuffer.allocate(idField.getSize());
        field1Buffer = ByteBuffer.allocate(field1.getSize());
        field2Buffer = ByteBuffer.allocate(field2.getSize());
    }

}
