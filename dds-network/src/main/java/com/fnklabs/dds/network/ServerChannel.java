package com.fnklabs.dds.network;

import com.fnklabs.dds.network.pool.ServerExecutor;

public interface ServerChannel extends Channel {
    void join(ServerExecutor executor);
}
