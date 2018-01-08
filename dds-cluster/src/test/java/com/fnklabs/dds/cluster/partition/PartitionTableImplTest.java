package com.fnklabs.dds.cluster.partition;

import com.fnklabs.dds.cluster.Node;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

@RunWith(MockitoJUnitRunner.class)
public class PartitionTableImplTest {

    private PartitionTableImpl partitionTable;

    @Mock
    private Node node;

    @Mock
    private Node secondNode;

    private PartitionImpl partition;

    @Before
    public void setUp() throws Exception {
        partitionTable = new PartitionTableImpl();
        partition = new PartitionImpl(0, 10, PartitionState.OK);
        partitionTable.addPartition(partition, node);
    }

    @Test
    public void addPartition() {
        assertNull(partitionTable.getOwner(10));

        partitionTable.addPartition(new PartitionImpl(10, 12, PartitionState.OK), secondNode);

        assertEquals(secondNode, partitionTable.getOwner(10));
    }

    @Test
    public void getOwner() {
        Node owner = partitionTable.getOwner(partition);

        assertEquals(node, owner);
    }

    @Test
    public void getOwnerByToken() {
        assertNull(partitionTable.getOwner(-1));
        assertEquals(node, partitionTable.getOwner(0));
        assertEquals(node, partitionTable.getOwner(1));
        assertEquals(node, partitionTable.getOwner(9));
        assertNull(partitionTable.getOwner(10));


    }
}