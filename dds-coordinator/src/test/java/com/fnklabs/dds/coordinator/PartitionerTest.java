package com.fnklabs.dds.coordinator;

import org.junit.Assert;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.Set;

public class PartitionerTest {

    @Test
    public void testGetKey() throws Exception {
        BigInteger startToken = new BigInteger(Partitioner.MIN_TOKEN_VALUE);
        BigInteger endToken = new BigInteger(Partitioner.MAX_TOKEN_VALUE);

        LoggerFactory.getLogger(getClass()).debug("Min value: {} Max value: {}", startToken, endToken);
    }

    @Test
    public void testSplit() throws Exception {
        Set<Bucket> split = Partitioner.split(10);

        Assert.assertNotNull(split);

        for (Bucket bucket : split) {
            LoggerFactory.getLogger(getClass()).debug("Bucket: [{}  -  {}]", new BigInteger(bucket.getStart().getTokenValue()), new BigInteger(bucket.getEnd().getTokenValue()));
        }
    }

    @Test
    public void testMidPoint() throws Exception {
        BigInteger left = BigInteger.valueOf(10l);
        BigInteger right = BigInteger.valueOf(7l);

        Token token = Partitioner.midPoint(new Token(left.toByteArray()), new Token(right.toByteArray()));

        Assert.assertNotNull(token);
        Assert.assertEquals(8, new BigInteger(token.getTokenValue()).intValue());
    }

}