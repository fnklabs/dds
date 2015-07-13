package com.fnklabs.dds.coordinator;

import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.ListeningScheduledExecutorService;

public class TestNodeFactory extends DefaultNodeFactory {
    private HostAndPort localAddress;

    public TestNodeFactory(ListeningExecutorService executorService, ListeningScheduledExecutorService scheduledExecutorService, HostAndPort localAddress) {
        super(executorService, scheduledExecutorService);
        this.localAddress = localAddress;
    }

    @Override
    public boolean isLocal(HostAndPort address) {
        return localAddress.equals(address);
    }
}
