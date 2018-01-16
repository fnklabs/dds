package com.fnklabs.dds.storage.columnar;

import com.fnklabs.dds.table.*;
import com.fnklabs.dds.table.codec.CodecRegistry;
import com.google.protobuf.ByteString;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.ByteBuffer;

public class SelectFromChunkTest extends AbstractChunkTest {

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();


        CodecRegistry.get(DataType.STRING).encode("TEST", field2Buffer);

        for (long i = 0; i < 100; i++) {
            CodecRegistry.get(DataType.LONG).encode(i, idBuffer);
            CodecRegistry.get(DataType.LONG).encode(i + 1000, field1Buffer);

            Insert insert = Insert.newBuilder()
                                  .setTable("test")
                                  .putValue("id", ByteString.copyFrom(idBuffer.array()))
                                  .putValue("field-1", ByteString.copyFrom(field1Buffer.array()))
                                  .putValue("field-2", ByteString.copyFrom(field2Buffer.array()))
                                  .build();

            chunk.query(insert);

            idBuffer.clear();
            field1Buffer.clear();
            field2Buffer.clear();
        }

        idBuffer.clear();
        field1Buffer.clear();
        field2Buffer.clear();
    }


    @Test
    public void selectQueryWithoutCondition() {
        Select select = Select.newBuilder()
                              .addSelection(Selection.newBuilder().setColumn("id").setAggregateFunction(AggregationFunction.COUNT).build())
                              .setTable("test")
                              .build();

        ResultSet query = chunk.query(select);

        Assert.assertEquals(1, query.getResultCount());
        Assert.assertNotNull(query.getResult(0));

        Assert.assertTrue(query.getResult(0).containsValue("count"));
        ByteString countData = query.getResult(0).getValueOrThrow("count");

        Assert.assertNotNull(countData);
        Assert.assertEquals(100L, CodecRegistry.get(DataType.LONG).decode(countData.asReadOnlyByteBuffer()));
    }

    @Test
    public void selectByIdEq() {
        CodecRegistry.get(DataType.LONG).encode(1L, idBuffer);

        Select select = Select.newBuilder()
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

        ResultSet query = chunk.query(select);

        Assert.assertEquals(1, query.getResultCount());
        Assert.assertNotNull(query.getResult(0));

        Assert.assertTrue(query.getResult(0).containsValue("count"));
        ByteString countData = query.getResult(0).getValueOrThrow("count");

        Assert.assertNotNull(countData);
        Assert.assertEquals(1L, CodecRegistry.get(DataType.LONG).decode(countData.asReadOnlyByteBuffer()));
    }

    @Test
    public void selectByIdNeq() {
        CodecRegistry.get(DataType.LONG).encode(1L, idBuffer);

        Select select = Select.newBuilder()
                              .addSelection(Selection.newBuilder().setColumn("id").setAggregateFunction(AggregationFunction.COUNT).build())
                              .setTable("test")
                              .setWhere(
                                      Where.newBuilder()
                                           .addClauses(
                                                   Clause.newBuilder()
                                                         .setColumn("id")
                                                         .setExpression(Expression.NEQ)
                                                         .setValue(ByteString.copyFrom(idBuffer.array())).build()
                                           )
                                           .build()
                              )
                              .build();

        ResultSet query = chunk.query(select);

        Assert.assertEquals(1, query.getResultCount());
        Assert.assertNotNull(query.getResult(0));

        Assert.assertTrue(query.getResult(0).containsValue("count"));
        ByteString countData = query.getResult(0).getValueOrThrow("count");

        Assert.assertNotNull(countData);
        Assert.assertEquals(99L, CodecRegistry.get(DataType.LONG).decode(countData.asReadOnlyByteBuffer()));
    }

    @Test
    public void selectByField1AndField2() {
        CodecRegistry.get(DataType.LONG).encode(1001L, field1Buffer);
        CodecRegistry.get(DataType.STRING).encode("TEST", field2Buffer);

        Select select = Select.newBuilder()
                              .addSelection(Selection.newBuilder().setColumn("id").setAggregateFunction(AggregationFunction.COUNT).build())
                              .setTable("test")
                              .setWhere(
                                      Where.newBuilder()
                                           .addClauses(
                                                   Clause.newBuilder()
                                                         .setColumn("field-1")
                                                         .setExpression(Expression.EQ)
                                                         .setValue(ByteString.copyFrom(field1Buffer.array())).build()
                                           )
                                           .addClauses(
                                                   Clause.newBuilder()
                                                         .setColumn("field-2")
                                                         .setExpression(Expression.EQ)
                                                         .setValue(ByteString.copyFrom(field2Buffer.array())).build()
                                           )
                                           .build()
                              )
                              .build();

        ResultSet query = chunk.query(select);

        Assert.assertEquals(1, query.getResultCount());
        Assert.assertNotNull(query.getResult(0));

        Assert.assertTrue(query.getResult(0).containsValue("count"));
        ByteString countData = query.getResult(0).getValueOrThrow("count");

        Assert.assertNotNull(countData);
        Assert.assertEquals(1L, CodecRegistry.get(DataType.LONG).decode(countData.asReadOnlyByteBuffer()));
    }

    @Test
    public void selectByField2() {
        CodecRegistry.get(DataType.STRING).encode("TEST", field2Buffer);

        Select select = Select.newBuilder()
                              .addSelection(Selection.newBuilder().setColumn("id").setAggregateFunction(AggregationFunction.COUNT).build())
                              .setTable("test")
                              .setWhere(
                                      Where.newBuilder()
                                           .addClauses(
                                                   Clause.newBuilder()
                                                         .setColumn("field-2")
                                                         .setExpression(Expression.EQ)
                                                         .setValue(ByteString.copyFrom(field2Buffer.array())).build()
                                           )
                                           .build()
                              )
                              .build();

        ResultSet query = chunk.query(select);

        Assert.assertEquals(1, query.getResultCount());
        Assert.assertNotNull(query.getResult(0));

        Assert.assertTrue(query.getResult(0).containsValue("count"));
        ByteString countData = query.getResult(0).getValueOrThrow("count");

        Assert.assertNotNull(countData);
        Assert.assertEquals(100L, CodecRegistry.get(DataType.LONG).decode(countData.asReadOnlyByteBuffer()));
    }
}