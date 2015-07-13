package com.fnklabs.dds.coordinator;

import org.junit.Assert;
import org.junit.Test;

public class RingTest {

    @Test
    public void testGetKeyId() throws Exception {
        byte b = new Integer(256).byteValue();
        byte[] key1 = {0, 1, 'a', (byte) '±', Byte.MAX_VALUE, Byte.MIN_VALUE};
        byte[] keyId = Partitioner.hash(key1);

        String str = "0x";

        for (byte key : key1) {
            int i = Byte.toUnsignedInt(key);
            int i1 = key & 0xFF;
            str += " " + Integer.toHexString(i1);

        }

        Assert.assertNotNull(keyId);
    }
}