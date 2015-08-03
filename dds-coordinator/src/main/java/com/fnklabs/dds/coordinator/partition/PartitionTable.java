package com.fnklabs.dds.coordinator.partition;

import com.google.common.collect.Sets;
import org.jetbrains.annotations.NotNull;

import java.io.Serializable;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiFunction;

/**
 * Contains data about partition - who own partitions
 */
public class PartitionTable implements Serializable {
    @NotNull
    private final Map<Partition, Set<UUID>> partition = new ConcurrentHashMap<>();

    public PartitionTable() {
    }

    @NotNull
    public Map<Partition, Set<UUID>> getPartition() {
        return partition;
    }

    public void addPartition(Partition partition, UUID member) {
        this.partition.compute(partition, new BiFunction<Partition, Set<UUID>, Set<UUID>>() {
            @Override
            public Set<UUID> apply(Partition partition, Set<UUID> members) {

                if (members != null) {
                    members.add(member);
                } else {
                    members = Sets.newHashSet(member);
                }

                return members;
            }
        });
    }
}
