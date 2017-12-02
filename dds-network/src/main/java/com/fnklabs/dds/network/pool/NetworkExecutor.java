package com.fnklabs.dds.network.pool;

import com.fnklabs.dds.network.Channel;

import java.io.Closeable;

/**
 * Network pool for
 */
public interface NetworkExecutor<T extends Channel> extends Closeable {

    void run();

    void shutdown();

}
