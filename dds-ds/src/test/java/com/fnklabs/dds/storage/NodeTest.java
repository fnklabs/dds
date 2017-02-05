package com.fnklabs.dds.storage;

import com.fnklabs.dds.BytesUtils;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigInteger;

public class NodeTest {


    @Test
    public void notEquals() throws Exception {
        Node firstNode = Node.builder()
                             .withKey(new BigInteger("1").toByteArray())
                             .build();

        Node secondNode = Node.builder()
                              .withKey(new BigInteger("2").toByteArray())
                              .build();

        Assert.assertNotEquals(firstNode, secondNode);
        Assert.assertFalse(firstNode.equals(secondNode));
    }

    @Test
    public void equals() throws Exception {
        Node firstNode = Node.builder()
                             .withKey(new BigInteger("1").toByteArray())
                             .build();

        Node secondNode = Node.builder()
                              .withKey(new BigInteger("1").toByteArray())
                              .build();

        Assert.assertEquals(firstNode, secondNode);
        Assert.assertTrue(firstNode.equals(secondNode));
    }

    @Test
    public void getRecordLength() throws Exception {

    }

    @Test
    public void getKey() throws Exception {

    }

    @Test
    public void getPosition() throws Exception {

    }

    @Test
    public void getDataReference() throws Exception {

    }

    @Test
    public void getLeftNodeReference() throws Exception {

    }

    @Test
    public void getRightNodeReference() throws Exception {

    }

    @Test
    public void hasChild() throws Exception {

    }

}