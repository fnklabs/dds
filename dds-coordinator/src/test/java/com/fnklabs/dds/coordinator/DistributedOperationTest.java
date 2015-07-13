package com.fnklabs.dds.coordinator;

import com.fnklabs.dds.coordinator.operation.DistributedOperation;
import com.fnklabs.dds.coordinator.operation.OperationOptions;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;
import java.util.function.Function;

import static org.mockito.Mockito.*;

public class DistributedOperationTest {

    @Test
    public void testExecuteWithConsistencyAll() throws Exception {
        SettableFuture<Boolean> firstFuture = SettableFuture.<Boolean>create();
        SettableFuture<Boolean> secondFuture = SettableFuture.<Boolean>create();
        firstFuture.set(true);
        secondFuture.set(true);

        NodeInfo firstNode = Mockito.mock(NodeInfo.class);
        NodeInfo secondNode = Mockito.mock(NodeInfo.class);

        LoadBalancingPolicy loadBalancingPolicy = Mockito.mock(LoadBalancingPolicy.class);
        when(loadBalancingPolicy.getExecutionPlan()).thenReturn(Arrays.asList(firstNode, secondNode));

        Function function = Mockito.mock(Function.class);

        Mockito.when(function.apply(firstNode)).thenReturn(firstFuture);
        Mockito.when(function.apply(secondNode)).thenReturn(secondFuture);


        ListenableFuture<Boolean> executeFuture = DistributedOperation.execute(new OperationOptions(ConsistencyLevel.ALL, loadBalancingPolicy, OperationOptions.DEFAULT_RETRY_COUNT), function);

        Boolean result = Futures.get(executeFuture, Exception.class);

        Assert.assertTrue(result);


        firstFuture = SettableFuture.<Boolean>create();
        secondFuture = SettableFuture.<Boolean>create();
        firstFuture.set(true);
        secondFuture.set(false);

        firstNode = Mockito.mock(NodeInfo.class);
        secondNode = Mockito.mock(NodeInfo.class);

        loadBalancingPolicy = Mockito.mock(LoadBalancingPolicy.class);
        when(loadBalancingPolicy.getExecutionPlan()).thenReturn(Arrays.asList(firstNode, secondNode));

        function = Mockito.mock(Function.class);

        Mockito.when(function.apply(firstNode)).thenReturn(firstFuture);
        Mockito.when(function.apply(secondNode)).thenReturn(secondFuture);


        executeFuture = DistributedOperation.execute(new OperationOptions(ConsistencyLevel.ALL, loadBalancingPolicy, OperationOptions.DEFAULT_RETRY_COUNT), function);

        result = Futures.get(executeFuture, Exception.class);

        Assert.assertFalse(result);


        firstFuture = SettableFuture.<Boolean>create();
        secondFuture = SettableFuture.<Boolean>create();
        firstFuture.set(false);
        secondFuture.set(false);

        firstNode = Mockito.mock(NodeInfo.class);
        secondNode = Mockito.mock(NodeInfo.class);

        loadBalancingPolicy = Mockito.mock(LoadBalancingPolicy.class);
        when(loadBalancingPolicy.getExecutionPlan()).thenReturn(Arrays.asList(firstNode, secondNode));

        function = Mockito.mock(Function.class);

        Mockito.when(function.apply(firstNode)).thenReturn(firstFuture);
        Mockito.when(function.apply(secondNode)).thenReturn(secondFuture);


        executeFuture = DistributedOperation.execute(new OperationOptions(ConsistencyLevel.ALL, loadBalancingPolicy, OperationOptions.DEFAULT_RETRY_COUNT), function);

        result = Futures.get(executeFuture, Exception.class);

        Assert.assertFalse(result);


    }

    @Test
    public void testExecuteWithConsistencyQuorum() throws Exception {
        SettableFuture<Boolean> firstFuture = SettableFuture.<Boolean>create();
        SettableFuture<Boolean> secondFuture = SettableFuture.<Boolean>create();
        SettableFuture<Boolean> thirdFuture = SettableFuture.<Boolean>create();
        firstFuture.set(true);
        secondFuture.set(true);
        thirdFuture.set(true);

        NodeInfo firstNode = Mockito.mock(NodeInfo.class);
        NodeInfo secondNode = Mockito.mock(NodeInfo.class);
        NodeInfo thirdNode = Mockito.mock(NodeInfo.class);

        LoadBalancingPolicy loadBalancingPolicy = Mockito.mock(LoadBalancingPolicy.class);
        when(loadBalancingPolicy.getExecutionPlan()).thenReturn(Arrays.asList(firstNode, secondNode, thirdNode));

        Function function = Mockito.mock(Function.class);

        Mockito.when(function.apply(firstNode)).thenReturn(firstFuture);
        Mockito.when(function.apply(secondNode)).thenReturn(secondFuture);
        Mockito.when(function.apply(thirdNode)).thenReturn(thirdFuture);


        ListenableFuture<Boolean> executeFuture = DistributedOperation.execute(new OperationOptions(ConsistencyLevel.QUORUM, loadBalancingPolicy, OperationOptions.DEFAULT_RETRY_COUNT), function);

        Boolean result = Futures.get(executeFuture, Exception.class);

        Assert.assertTrue(result);

        verify(function, times(1)).apply(firstNode);
        verify(function, times(1)).apply(secondNode);
        verify(function, times(1)).apply(thirdNode);


        firstFuture = SettableFuture.<Boolean>create();
        secondFuture = SettableFuture.<Boolean>create();
        thirdFuture = SettableFuture.<Boolean>create();
        firstFuture.set(true);
        secondFuture.set(false);
        thirdFuture.set(true);

        firstNode = Mockito.mock(NodeInfo.class);
        secondNode = Mockito.mock(NodeInfo.class);
        thirdNode = Mockito.mock(NodeInfo.class);

        loadBalancingPolicy = Mockito.mock(LoadBalancingPolicy.class);
        when(loadBalancingPolicy.getExecutionPlan()).thenReturn(Arrays.asList(firstNode, secondNode, thirdNode));

        function = Mockito.mock(Function.class);

        Mockito.when(function.apply(firstNode)).thenReturn(firstFuture);
        Mockito.when(function.apply(secondNode)).thenReturn(secondFuture);
        Mockito.when(function.apply(thirdNode)).thenReturn(thirdFuture);


        executeFuture = DistributedOperation.execute(new OperationOptions(ConsistencyLevel.QUORUM, loadBalancingPolicy, OperationOptions.DEFAULT_RETRY_COUNT), function);

        result = Futures.get(executeFuture, Exception.class);

        Assert.assertTrue(result);

        verify(function, times(1)).apply(firstNode);
        verify(function, times(1)).apply(secondNode);
        verify(function, times(1)).apply(thirdNode);


        firstFuture = SettableFuture.<Boolean>create();
        secondFuture = SettableFuture.<Boolean>create();
        thirdFuture = SettableFuture.<Boolean>create();
        firstFuture.set(true);
        secondFuture.set(false);
        thirdFuture.set(false);

        firstNode = Mockito.mock(NodeInfo.class);
        secondNode = Mockito.mock(NodeInfo.class);
        thirdNode = Mockito.mock(NodeInfo.class);

        loadBalancingPolicy = Mockito.mock(LoadBalancingPolicy.class);
        when(loadBalancingPolicy.getExecutionPlan()).thenReturn(Arrays.asList(firstNode, secondNode, thirdNode));

        function = Mockito.mock(Function.class);

        Mockito.when(function.apply(firstNode)).thenReturn(firstFuture);
        Mockito.when(function.apply(secondNode)).thenReturn(secondFuture);
        Mockito.when(function.apply(thirdNode)).thenReturn(thirdFuture);


        executeFuture = DistributedOperation.execute(new OperationOptions(ConsistencyLevel.QUORUM, loadBalancingPolicy, OperationOptions.DEFAULT_RETRY_COUNT), function);

        result = Futures.get(executeFuture, Exception.class);

        Assert.assertFalse(result);

        verify(function, times(1)).apply(firstNode);
        verify(function, times(1)).apply(secondNode);
        verify(function, times(1)).apply(thirdNode);


        firstFuture = SettableFuture.<Boolean>create();
        secondFuture = SettableFuture.<Boolean>create();
        thirdFuture = SettableFuture.<Boolean>create();
        firstFuture.set(false);
        secondFuture.set(false);
        thirdFuture.set(false);

        firstNode = Mockito.mock(NodeInfo.class);
        secondNode = Mockito.mock(NodeInfo.class);
        thirdNode = Mockito.mock(NodeInfo.class);

        loadBalancingPolicy = Mockito.mock(LoadBalancingPolicy.class);
        when(loadBalancingPolicy.getExecutionPlan()).thenReturn(Arrays.asList(firstNode, secondNode, thirdNode));

        function = Mockito.mock(Function.class);

        Mockito.when(function.apply(firstNode)).thenReturn(firstFuture);
        Mockito.when(function.apply(secondNode)).thenReturn(secondFuture);
        Mockito.when(function.apply(thirdNode)).thenReturn(thirdFuture);


        executeFuture = DistributedOperation.execute(new OperationOptions(ConsistencyLevel.QUORUM, loadBalancingPolicy, OperationOptions.DEFAULT_RETRY_COUNT), function);

        result = Futures.get(executeFuture, Exception.class);

        Assert.assertFalse(result);

        verify(function, times(1)).apply(firstNode);
        verify(function, times(1)).apply(secondNode);
        verify(function, times(1)).apply(thirdNode);
    }
}