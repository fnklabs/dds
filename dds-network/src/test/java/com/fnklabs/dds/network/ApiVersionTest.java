package com.fnklabs.dds.network;

import org.junit.Assert;
import org.junit.Test;

public class ApiVersionTest {

    @Test(expected = IllegalArgumentException.class)
    public void testValueOfWithException() throws Exception {
        ApiVersion.valueOf(-1);
    }

    @Test()
    public void testValueOf() throws Exception {
        ApiVersion apiVersion = ApiVersion.valueOf(ApiVersion.VERSION_1.getVersion());

        Assert.assertEquals(ApiVersion.VERSION_1, apiVersion);
    }
}