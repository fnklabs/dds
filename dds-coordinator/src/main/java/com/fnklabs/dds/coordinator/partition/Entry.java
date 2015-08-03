package com.fnklabs.dds.coordinator.partition;

import java.io.Serializable;
import java.util.Set;
import java.util.UUID;

public interface Entry<K, V> extends Serializable {

    /**
     * Get partition entry key
     *
     * @return Record key
     */
    PartitionKey getPartitionKey();

    /**
     * Get user entry key
     *
     * @return Key of current entry
     */
    K getKey();

    /**
     * Get entry value
     *
     * @return Value
     */
    V getValue();

    /**
     * Get set of node id (owners) who own current entry
     *
     * @return Set of node id
     */
    Set<UUID> getEntryOwners();
}
