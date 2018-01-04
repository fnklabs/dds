package com.fnklabs.dds.storage.columnar;

import com.fnklabs.dds.storage.Record;
import com.fnklabs.dds.storage.column.Column;
import com.fnklabs.dds.storage.column.IntegerColumn;
import com.fnklabs.dds.storage.column.LongColumn;
import com.fnklabs.dds.storage.im.ImStorageFactory;
import com.fnklabs.dds.storage.query.Condition;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;

public class ColumnarTableTest {
    private ColumnarTable columnarTable;

    private LongColumn idColumn;
    private LongColumn createdAtColumn;
    private IntegerColumn priceColumn;

    @Before
    public void setUp() throws Exception {
        idColumn = new LongColumn("id", (short) 0, true);
        createdAtColumn = new LongColumn("created_at", (short) 1);
        priceColumn = new IntegerColumn("price", (short) 2);

        columnarTable = new ColumnarTable(
                "test",
                Arrays.asList(
                        idColumn,
                        createdAtColumn,
                        priceColumn
                ),
                4,
                64 * 1024 * 1024,
                new ImStorageFactory()
        );
    }

    @Test
    public void name() {
        Assert.assertEquals("test", columnarTable.name());
    }

    @Test
    public void chunks() {
    }

    @Test
    public void map() {
    }

    @Test
    public void write() {
        Record record = new Record(new HashMap<Column, Object>() {{
            put(idColumn, 1L);
            put(createdAtColumn, 2L);
            put(priceColumn, 3);
        }});

        columnarTable.write(record);
    }

    @Test
    public void readNull() {
        Record record = columnarTable.read(new byte[]{1});

        Assert.assertNull(record);
    }

    @Test
    public void read() {
        Record record1 = new Record(new HashMap<Column, Object>() {{
            put(idColumn, 1L);
            put(createdAtColumn, 2L);
            put(priceColumn, 3);
        }});

        Record record2 = new Record(new HashMap<Column, Object>() {{
            put(idColumn, 2L);
            put(createdAtColumn, 5L);
            put(priceColumn, 6);
        }});

        columnarTable.write(record1);
        columnarTable.write(record2);

        ByteBuffer keyBuffer = ByteBuffer.allocate(Long.BYTES);
        keyBuffer.putLong(1L);
        keyBuffer.rewind();

        Record record = columnarTable.read(keyBuffer.array());

        Assert.assertNotNull(record);
        Assert.assertEquals(Long.valueOf(1L), record.<Long>get(idColumn));
        Assert.assertEquals(Long.valueOf(2L), record.get(createdAtColumn));
        Assert.assertEquals(Integer.valueOf(3), record.get(priceColumn));


        keyBuffer.clear();
        keyBuffer.putLong(2L);
        keyBuffer.rewind();

        record = columnarTable.read(keyBuffer.array());

        Assert.assertNotNull(record);
        Assert.assertEquals(Long.valueOf(2L), record.<Long>get(idColumn));
        Assert.assertEquals(Long.valueOf(5L), record.get(createdAtColumn));
        Assert.assertEquals(Integer.valueOf(6), record.get(priceColumn));
    }

    @Test
    public void query() {
        for (long i = 0; i < 100; i++) {
            long value = i % 10;
            Record record = new Record(new HashMap<Column, Object>() {{
                put(idColumn, value);
                put(createdAtColumn, 1L);
                put(priceColumn, 1);
            }});

            columnarTable.write(record);
        }

        Integer result = columnarTable.query(
                priceColumn.name(),
                new Condition<Long>(o -> o.equals(3L)),
                new PriceTotal.SumPrice()
        );

        Assert.assertEquals(10, result.intValue());
    }
}