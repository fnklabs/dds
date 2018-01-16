package com.fnklabs.dds.storage.columnar;

import com.fnklabs.dds.table.Insert;
import com.fnklabs.dds.table.ResultSet;
import com.google.protobuf.ByteString;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class InsertIntoChunkTest extends AbstractChunkTest {

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();

        idBuffer.putLong(1L);
        field1Buffer.putLong(2L);
        field2Buffer.put("TEST".getBytes());
    }

    @Test
    public void insertQuery() {
        Insert insert = Insert.newBuilder()
                              .setTable("test")
                              .putValue("id", ByteString.copyFrom(idBuffer.array()))
                              .putValue("field-1", ByteString.copyFrom(field1Buffer.array()))
                              .putValue("field-2", ByteString.copyFrom(field2Buffer.array()))
                              .build();


        ResultSet query = chunk.query(insert);

        Assert.assertTrue(query.getWasApplied());
    }
}