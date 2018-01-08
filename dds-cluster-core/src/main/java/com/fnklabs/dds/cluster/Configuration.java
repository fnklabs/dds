package com.fnklabs.dds.cluster;

import com.google.common.net.HostAndPort;
import org.jetbrains.annotations.Nullable;

import java.util.Set;
import java.util.UUID;

/**
 * Node configuration
 */
public interface Configuration {
    @Nullable
    UUID currentId();

    Set<HostAndPort> seeds();

    int replicationFactor();

    HostAndPort listenAddress();

    default int getWorkerPoolSize() {
        return 1;
    }

    default int getNioPoolSize() {
        return 1;
    }

    default int getNetworkPoolSize() {
        return 1;
    }
}
