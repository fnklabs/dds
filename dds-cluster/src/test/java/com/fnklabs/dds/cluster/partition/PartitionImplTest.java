package com.fnklabs.dds.cluster.partition;

import com.google.common.collect.Range;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class PartitionImplTest {

    private PartitionImpl partition;

    @Before
    public void setUp() throws Exception {
        partition = new PartitionImpl(0, 10, PartitionState.OK);
    }

    @Test
    public void range() {
        Range<Long> range = partition.range();

        assertEquals(Range.<Long>closedOpen(0L, 10L), range);
    }

    @Test
    public void state() {
        assertEquals(PartitionState.OK, partition.state());
    }

    @Test
    public void owned() {
        assertFalse(partition.owned(-1));
        assertTrue(partition.owned(0));
        assertTrue(partition.owned(1));
        assertTrue(partition.owned(9));
        assertFalse(partition.owned(10));
    }
}