package com.fnklabs.dds.network;

import org.junit.Test;

public class StatusCodeTest {

    @Test(expected = IllegalArgumentException.class)
    public void testValueOfWithException() throws Exception {
        StatusCode statusCode = StatusCode.valueOf(-1);
    }

    @Test
    public void testValueOf() throws Exception {
        StatusCode.valueOf(StatusCode.OK.value());
    }
}