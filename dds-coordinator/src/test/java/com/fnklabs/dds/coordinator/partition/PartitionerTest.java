package com.fnklabs.dds.coordinator.partition;

import com.google.common.collect.Sets;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.util.TreeSet;
import java.util.UUID;

public class PartitionerTest {

    public void testBuildToken() throws Exception {

    }

    public void testMidPoint() throws Exception {

    }

    public void testSplit() throws Exception {

    }

    @Test
    public void testBuildPartitionTable() throws Exception {
        TreeSet<UUID> members = new TreeSet<>(Sets.newHashSet(
                UUID.randomUUID(),
                UUID.randomUUID(),
                UUID.randomUUID(),
                UUID.randomUUID()
        ));

        PartitionTable partitionTable = Partitioner.buildPartitionTable(members, 2);

        partitionTable.getPartition().forEach((key, value) -> {
            LoggerFactory.getLogger(getClass()).debug("Partition: {}: [{}]", key, value);
        });

    }

    public void testHash() throws Exception {

    }
}