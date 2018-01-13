package com.fnklabs.dds.storage.row;

import com.fnklabs.dds.storage.Record;
import com.fnklabs.dds.storage.column.Column;
import com.fnklabs.dds.storage.column.IntegerColumn;
import com.fnklabs.dds.storage.column.LongColumn;
import com.fnklabs.dds.storage.im.ImStorageFactory;
import com.fnklabs.dds.table.query.Condition;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;

public class RowTableTest {
    private RowTable rowTable;

    private LongColumn idColumn;
    private LongColumn createdAtColumn;
    private IntegerColumn priceColumn;

    @Before
    public void setUp() throws Exception {
        idColumn = new LongColumn("id", (short) 0, true);
        createdAtColumn = new LongColumn("created_at", (short) 1);
        priceColumn = new IntegerColumn("price", (short) 2);

        rowTable = new RowTable(
                "test",
                Arrays.asList(
                        idColumn,
                        createdAtColumn,
                        priceColumn
                ),
                64 * 1024 * 1024,
                new ImStorageFactory()
        );
    }

    @Test
    public void name() {
        Assert.assertEquals("test", rowTable.name());
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

        rowTable.write(record);
    }

    @Test
    public void readNull() {
        Record record = rowTable.read(new byte[]{1});

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

        rowTable.write(record1);
        rowTable.write(record2);

        ByteBuffer keyBuffer = ByteBuffer.allocate(Long.BYTES);
        keyBuffer.putLong(1L);
        keyBuffer.rewind();

        Record record = rowTable.read(keyBuffer.array());

        Assert.assertNotNull(record);
        Assert.assertEquals(Long.valueOf(1L), record.<Long>get(idColumn));
        Assert.assertEquals(Long.valueOf(2L), record.get(createdAtColumn));
        Assert.assertEquals(Integer.valueOf(3), record.get(priceColumn));


        keyBuffer.clear();
        keyBuffer.putLong(2L);
        keyBuffer.rewind();

        record = rowTable.read(keyBuffer.array());

        Assert.assertNotNull(record);
        Assert.assertEquals(Long.valueOf(2L), record.<Long>get(idColumn));
        Assert.assertEquals(Long.valueOf(5L), record.get(createdAtColumn));
        Assert.assertEquals(Integer.valueOf(6), record.get(priceColumn));
    }

    @Test
    public void query() {
        for (long i = 0; i < 1_000_000; i++) {
            long value = i % 100;
            Record record = new Record(new HashMap<Column, Object>() {{
                put(idColumn, value);
                put(createdAtColumn, 1L);
                put(priceColumn, 1);
            }});

            rowTable.write(record);
        }

        for (int i = 0; i < 4; i++) {
            Integer result = rowTable.query(
                    priceColumn.name(),
                    new Condition<Long>(o -> o.equals(3L)),
                    new PriceTotal.SumPrice()
            );

            Assert.assertEquals(10_000, result.intValue());
        }
    }
}