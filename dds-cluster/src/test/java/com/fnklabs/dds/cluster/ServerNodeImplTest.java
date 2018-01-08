package com.fnklabs.dds.cluster;

import com.google.common.net.HostAndPort;
import org.junit.*;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.io.IOException;

import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
@Ignore
public class ServerNodeImplTest {
    private ServerNodeImpl serverNode;

    @Mock
    private Configuration configuration;

    @Before
    public void setUp() throws Exception {
        when(configuration.getNetworkPoolSize()).thenReturn(1);
        when(configuration.getNioPoolSize()).thenReturn(1);
        when(configuration.getWorkerPoolSize()).thenReturn(1);

        when(configuration.listenAddress()).thenReturn(HostAndPort.fromParts("127.0.0.1", 10_000));

        serverNode = new ServerNodeImpl(configuration);
    }

    @After
    public void tearDown() throws Exception {
        serverNode.close();
    }

    @Test
    public void start() throws IOException {
        serverNode.start();
    }

    @Test
    public void startUp() {
        NodeStatus nodeStatus = serverNode.getNodeStatus();

        Assert.assertEquals(NodeStatus.START_UP, nodeStatus);
    }
}