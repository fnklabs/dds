package com.fnklabs.dds.cluster.partition;

import com.fnklabs.dds.cluster.Node;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Contains data about partitionOwners - who own partitions
 */
class PartitionTableImpl implements PartitionTable {
    private final Map<Partition, List<Node>> partitionOwners = new ConcurrentHashMap<>();


    void addPartition(Partition partition, Node node) {
        partitionOwners.compute(partition, (p, o) -> {
            if (o == null) {
                o = new ArrayList<>();
            }

            o.add(node);

            return o;
        });
    }

    @Nullable
    @Override
    public Node getOwner(Partition partition) {
        List<Node> owners = this.partitionOwners.get(partition);

        return owners.isEmpty() ? null : owners.get(0);
    }

    @Nullable
    @Override
    public Node getOwner(long token) {
        for (Map.Entry<Partition, List<Node>> partitionListEntry : partitionOwners.entrySet()) {
            Partition key = partitionListEntry.getKey();

            if (key.owned(token)) {
                return partitionListEntry.getValue().get(0);
            }
        }

        return null;
    }
}
