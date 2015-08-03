package com.fnklabs.dds.coordinator;

import com.google.common.net.HostAndPort;
import org.jetbrains.annotations.NotNull;

import java.util.Set;
import java.util.UUID;

/**
 * Node configuration
 */
interface Configuration {
    /**
     * Node id that was previously retrieved or Randomly generated UUID if node was not registered in the cluster
     * <p>
     * That id will be used as node id
     *
     * @return Node id
     */
    @NotNull
    UUID getNodeId();

    /**
     * Get seeds
     *
     * @return Set of seeds
     */
    @NotNull
    Set<HostAndPort> getSeeds();

    /**
     * Get default replication factor
     *
     * @return Replication factor
     */
    default int getReplicationFactor() {
        return 1;
    }
}
