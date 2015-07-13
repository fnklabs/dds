package com.fnklabs.dds.coordinator;

import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

public class DefaultNodeFactoryTest {

    @Test
    public void testIsLocal() throws Exception {
        DefaultNodeFactory defaultNodeFactory = new DefaultNodeFactory(Mockito.mock(ListeningExecutorService.class), Mockito.mock(ListeningScheduledExecutorService.class));

        boolean local = defaultNodeFactory.isLocal(HostAndPort.fromHost("127.0.0.1"));

        Assert.assertTrue(local);

        local = defaultNodeFactory.isLocal(HostAndPort.fromHost("127.0.0.2"));

        Assert.assertFalse(local);
    }

    @Test
    public void testGet() throws Exception {
        DefaultNodeFactory defaultNodeFactory = new DefaultNodeFactory(Mockito.mock(ListeningExecutorService.class), Mockito.mock(ListeningScheduledExecutorService.class));

        Node node = defaultNodeFactory.get(HostAndPort.fromHost("127.0.0.1"));

        Assert.assertNotNull(node);

        ((LocalNode) node).shutdown();


    }
}