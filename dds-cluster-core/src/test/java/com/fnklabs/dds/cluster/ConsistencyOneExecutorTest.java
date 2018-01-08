package com.fnklabs.dds.cluster;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.SortedSet;
import java.util.TreeSet;

@RunWith(MockitoJUnitRunner.class)
public class ConsistencyOneExecutorTest {
    private ConsistencyOneExecutor consistencyOneExecutor;

    @Mock
    private Node firstNode;

    @Mock
    private Node secondNode;

    private SortedSet<Node> nodes;

    @Before
    public void setUp() throws Exception {
        consistencyOneExecutor = new ConsistencyOneExecutor();

        Mockito.when(secondNode.compareTo(firstNode)).thenReturn(-1);

        nodes = new TreeSet<>(Arrays.asList(firstNode, secondNode));
    }


    @Test
    public void executeOk() {
        Boolean execute = consistencyOneExecutor.execute(nodes, node -> true);

        Assert.assertTrue(execute);
    }

    @Test(expected = InconsistientException.class)
    public void executeFailed() {
        Boolean execute = consistencyOneExecutor.execute(nodes, node -> {throw new RuntimeException();});
    }
}