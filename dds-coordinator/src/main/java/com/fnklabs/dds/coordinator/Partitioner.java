package com.fnklabs.dds.coordinator;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.slf4j.LoggerFactory;

import java.math.BigInteger;
import java.util.HashSet;
import java.util.Set;

public class Partitioner {
    public final static byte[] MIN_TOKEN_VALUE = new byte[]{
            Byte.MIN_VALUE, Byte.MIN_VALUE, Byte.MIN_VALUE, Byte.MIN_VALUE,
            Byte.MIN_VALUE, Byte.MIN_VALUE, Byte.MIN_VALUE, Byte.MIN_VALUE,
            Byte.MIN_VALUE, Byte.MIN_VALUE, Byte.MIN_VALUE, Byte.MIN_VALUE,
            Byte.MIN_VALUE, Byte.MIN_VALUE, Byte.MIN_VALUE, Byte.MIN_VALUE,
    };
    public final static byte[] MAX_TOKEN_VALUE = new byte[]{
            Byte.MAX_VALUE, Byte.MAX_VALUE, Byte.MAX_VALUE, Byte.MAX_VALUE,
            Byte.MAX_VALUE, Byte.MAX_VALUE, Byte.MAX_VALUE, Byte.MAX_VALUE,
            Byte.MAX_VALUE, Byte.MAX_VALUE, Byte.MAX_VALUE, Byte.MAX_VALUE,
            Byte.MAX_VALUE, Byte.MAX_VALUE, Byte.MAX_VALUE, Byte.MAX_VALUE,
    };

    public final static int TOKEN_LENGTH = 16; // 128 bit or 16 byte
    /**
     * Record key hash function
     */
    private static final HashFunction hashFunction = Hashing.murmur3_128();

    public static Token buildToken(byte[] key) {
        byte[] token = hash(key);

        return new Token(token);
    }

    public static Token midPoint(Token left, Token right) {
        BigInteger halfOfLeft = new BigInteger(left.getTokenValue()).shiftRight(1);
        BigInteger halfOfRight = new BigInteger(right.getTokenValue()).shiftRight(1);


        return new Token(halfOfLeft.add(halfOfRight).toByteArray());
    }

    public static Set<Bucket> split(int nodes) {
        BigInteger step = new BigInteger(MAX_TOKEN_VALUE).shiftLeft(1).divide(BigInteger.valueOf(nodes));
        BigInteger maxValue = new BigInteger(MAX_TOKEN_VALUE);

        Set<Bucket> buckets = new HashSet<>();

        BigInteger leftBorder = new BigInteger(MIN_TOKEN_VALUE);

        for (int i = 0; i < nodes; i++) {
            BigInteger rightBorder = leftBorder.add(step);//.subtract(BigInteger.ONE);
            Bucket bucket;

            if (rightBorder.compareTo(maxValue) != 0) {
                bucket = new Bucket(new Token(leftBorder.toByteArray()), new Token(rightBorder.subtract(BigInteger.ONE).toByteArray()), Bucket.State.BALANCING);
            } else {
                bucket = new Bucket(new Token(leftBorder.toByteArray()), new Token(rightBorder.toByteArray()), Bucket.State.BALANCING);
            }

            buckets.add(bucket);

            leftBorder = rightBorder;
        }

        LoggerFactory.getLogger(Partitioner.class).debug("K: {}", step);


        return buckets;
    }

    /**
     * Get internal id of user key by hashing user key
     *
     * @param key User key
     *
     * @return Internal key
     */
    protected static byte[] hash(byte[] key) {
        HashCode hashCode = hashFunction.hashBytes(key);
        return hashCode.asBytes();
    }


}
