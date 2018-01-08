package com.fnklabs.dds.cluster.partition;

import com.fnklabs.dds.cluster.Node;
import org.jetbrains.annotations.Nullable;

public interface PartitionTable {
    @Nullable
    Node getOwner(Partition partition);

    @Nullable
    Node getOwner(long token);
}
