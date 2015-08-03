package com.fnklabs.dds.coordinator;

import org.junit.Test;
import org.mockito.Mockito;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.Mockito.*;

public class WatchDogTest {

    @Test
    public void testRunIfServerNodeIsNodeRunning() throws Exception {
        ServerNode serverNode = mock(ServerNode.class);
        AtomicBoolean isRunning = new AtomicBoolean(Boolean.FALSE);
        ExecutorService executorService = Mockito.mock(ExecutorService.class);


        WatchDog watchDog = new WatchDog(serverNode, isRunning, executorService);

        watchDog.run();

        verify(serverNode, never()).getNodeStatus();
        verify(executorService, never()).submit(any(Runnable.class));
    }

    @Test
    public void testRunIfServerNodeIsRunning() throws Exception {
        ServerNode serverNode = mock(ServerNode.class);
        AtomicBoolean isRunning = new AtomicBoolean(Boolean.TRUE);
        ExecutorService executorService = Mockito.mock(ExecutorService.class);


        WatchDog watchDog = new WatchDog(serverNode, isRunning, executorService);

        watchDog.run();

        verify(serverNode, times(1)).getNodeStatus();
        verify(executorService, times(1)).submit(watchDog);
    }

    @Test
    public void testRunWithDifferentStatus() throws Exception {
        ServerNode serverNode = mock(ServerNode.class);
        AtomicBoolean isRunning = new AtomicBoolean(Boolean.TRUE);
        ExecutorService executorService = Mockito.mock(ExecutorService.class);

        when(serverNode.getNodeStatus()).thenReturn(Node.NodeStatus.START_UP);

        WatchDog watchDog = new WatchDog(serverNode, isRunning, executorService);

        watchDog.run();

        // check start up call
        verify(serverNode, times(1)).getNodeStatus();
        verify(executorService, times(1)).submit(watchDog);
        verify(serverNode, times(1)).onStartUp();


        // check setup call

        serverNode = mock(ServerNode.class);

        when(serverNode.getNodeStatus()).thenReturn(Node.NodeStatus.SETUP);

        watchDog = new WatchDog(serverNode, isRunning, executorService);

        watchDog.run();

        verify(serverNode, times(1)).getNodeStatus();
        verify(executorService, times(1)).submit(watchDog);
        verify(serverNode, times(0)).onStartUp();
        verify(serverNode, times(1)).onSetUp();

        // check shutdown call

        serverNode = mock(ServerNode.class);

        when(serverNode.getNodeStatus()).thenReturn(Node.NodeStatus.SHUTDOWN);

        watchDog = new WatchDog(serverNode, isRunning, executorService);

        watchDog.run();

        verify(serverNode, times(1)).getNodeStatus();
        verify(executorService, times(1)).submit(watchDog);
        verify(serverNode, times(0)).onStartUp();
        verify(serverNode, times(0)).onSetUp();
        verify(serverNode, times(1)).close();

        // check rebalance call

        serverNode = mock(ServerNode.class);

        when(serverNode.getNodeStatus()).thenReturn(Node.NodeStatus.REPAIR);

        watchDog = new WatchDog(serverNode, isRunning, executorService);

        watchDog.run();

        verify(serverNode, times(1)).getNodeStatus();
        verify(executorService, times(1)).submit(watchDog);
        verify(serverNode, times(0)).onStartUp();
        verify(serverNode, times(0)).onSetUp();
        verify(serverNode, times(0)).close();
        verify(serverNode, times(1)).onRepair();
    }
}