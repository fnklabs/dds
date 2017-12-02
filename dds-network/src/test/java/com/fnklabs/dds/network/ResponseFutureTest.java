package com.fnklabs.dds.network;

import com.google.common.net.HostAndPort;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@Ignore
public class ResponseFutureTest {

//    @Test
//    public void testOnResponse() throws Exception {
//        BaseMessage message = Mockito.mock(BaseMessage.class);
//
//        ResponseFuture responseFuture = new ResponseFuture();
//        responseFuture.onResponse(message);
//
//        BaseMessage responseMessage = responseFuture.get(1, TimeUnit.SECONDS);
//
//        Assert.assertNotNull(responseMessage);
//        Assert.assertEquals(message, responseMessage);
//    }
//
//    @Test(expected = ExecutionException.class)
//    public void testOnException() throws Exception {
//        ResponseFuture responseFuture = new ResponseFuture();
//        responseFuture.onException(HostAndPort.fromString("127.0.0.1:8080"), new InterruptedException());
//        responseFuture.get(1, TimeUnit.SECONDS);
//    }
//
//    @Test(expected = ExecutionException.class)
//    public void testOnTimeout() throws Exception {
//        ResponseFuture responseFuture = new ResponseFuture();
//        responseFuture.onTimeout();
//        responseFuture.get(1, TimeUnit.SECONDS);
//    }
//
//    @Test
//    public void testIsExpired() throws Exception {
//        ResponseFuture responseFuture = new ResponseFuture();
//        Assert.assertFalse(responseFuture.isExpired());
//
//    }


}