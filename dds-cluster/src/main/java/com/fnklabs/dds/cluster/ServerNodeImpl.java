package com.fnklabs.dds.cluster;

import com.fnklabs.concurrent.Executors;
import com.fnklabs.dds.network.pool.NetworkExecutor;
import com.fnklabs.dds.network.pool.NioExecutor;
import com.fnklabs.dds.network.server.NetworkServer;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.jetbrains.annotations.Nullable;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.SortedSet;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Server node implementation
 */
class ServerNodeImpl implements Node {
    /**
     * Logger
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(ServerNodeImpl.class);

    private final UUID id = UUID.randomUUID();
    /**
     * Network server instance
     */
    private final NetworkServer networkServer;

    /**
     * Executor service
     */
    private final ExecutorService serverPool;

    /**
     * Is running status
     */

    private final AtomicBoolean isRunning = new AtomicBoolean(false);

    /**
     * Atomic reference to last actual node status
     */
    private final AtomicReference<NodeStatus> nodeStatus = new AtomicReference<>();

    /**
     *
     */
    private final ClientNodeRegistry clientNodeRegistry;

    /**
     * Cluster information reference
     */
    private final AtomicReference<Cluster> clusterInformationAtomicReference = new AtomicReference<>();

    private final NetworkExecutor networkExecutor;

    private final Configuration configuration;

    private final ClusterImpl cluster;

    /**
     * Construct new local node
     *
     * @param configuration Server node configuration
     */
    ServerNodeImpl(Configuration configuration) throws IOException {
        KryoSerializer serializer = new KryoSerializer();

        this.configuration = configuration;
        this.serverPool = Executors.fixedPoolExecutor(configuration.getWorkerPoolSize(), "dds.server.worker");
        this.clientNodeRegistry = new ClientNodeRegistry(configuration.getNetworkPoolSize(), serializer);

        this.networkExecutor = NioExecutor.builder()
                                          .setOpAcceptExecutor(configuration.getNioPoolSize())
                                          .setOpReadExecutor(configuration.getNioPoolSize())
                                          .setOpWriteExecutor(configuration.getNioPoolSize())
                                          .build();

        this.networkServer = new NetworkServer(configuration.listenAddress(), configuration.getNetworkPoolSize(), new ServerIncomeMessageHandler(this));

        for (int i = 0; i < configuration.getWorkerPoolSize(); i++) {
            getServerPool().submit(new WatchDog(this, isRunning));
        }
        cluster = new ClusterImpl(configuration.seeds(), clientNodeRegistry, serializer);
    }

    /**
     * Start server node
     */
    public void start() throws IOException {
        NodeStatus nodeStatus = this.nodeStatus.get();

        if (nodeStatus == null && this.nodeStatus.compareAndSet(null, NodeStatus.START_UP)) {
            isRunning.set(true);
            networkExecutor.run();

            networkServer.join(networkExecutor);

            SortedSet<Node> members = cluster.getMembers();

        }
    }

    @Override
    public void close() {
        LOGGER.warn("Shutting down node...");

        isRunning.set(false);

        serverPool.shutdown();

        try {
            serverPool.awaitTermination(60, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            LOGGER.warn("can't stop server", e);
        }

        networkExecutor.shutdown();
    }

    @Override
    public UUID getId() {
        return id;
    }

    @Override
    public NodeStatus getStatus() {
        return nodeStatus.get();
    }

    @Override
    public DateTime getLastUpdated() {
        return null;
    }

    @Override
    public HostAndPort getAddress() {
        return configuration.listenAddress();
    }

    @Override
    public Version getVersion() {
        return Version.current();
    }

    @Override
    public Configuration getConfiguration() {
        return configuration;
    }

    /**
     * Notify server that specified node was up or can be used to update information about node instead of using {@link
     * ServerNodeImpl#updateClusterInfo(Cluster)}  to update information about whole cluster.
     *
     * @param nodeInfo New node info
     *
     * @return new Cluster
     */
    @Override
    public ListenableFuture<Cluster> nodeUp(Node nodeInfo) {
        LOGGER.info("Node up: {}", nodeInfo);


        return null;
    }

    /**
     * Remove node from cluster information and start repair
     * <p>
     * todo complete and test realization
     * <p>
     * Can be called when node removed from cluster
     *
     * @param nodeInfo Node info
     *
     * @return Future for accepting operation
     */
    @Override
    public ListenableFuture<Boolean> nodeDown(Node nodeInfo) {
        LOGGER.info("Node down: {}", nodeInfo);

        return null;

    }

    /**
     * Update node status to start repair operation
     *
     * @param clusterInformation New cluster information by which repartition must be done
     *
     * @return Future for accepting operation
     */
    @Override
    public ListenableFuture<Boolean> repair(Cluster clusterInformation) {
        return null;
    }

    @Override
    public Node getNode() {
        return null;
    }

    /**
     * Get cluster information
     *
     * @return RingInfo
     */
    @Override
    public ListenableFuture<Boolean> update() {
        return null;
    }

    @Override
    public ListenableFuture<Boolean> updateClusterInfo(Cluster clusterInformation) {
        return Futures.immediateFuture(updateCluster(clusterInformation));
    }

    @Override
    public ListenableFuture<Long> ping() {
        SettableFuture<Long> pingFuture = SettableFuture.<Long>create();
        pingFuture.set(0l);
        return pingFuture;
    }

    @Nullable
    public Cluster getCluster() {
        return clusterInformationAtomicReference.get();
    }

    public boolean updateCluster(Cluster clusterInformation) {
        for (; ; ) {
            Cluster prevCluster = clusterInformationAtomicReference.get();

            if (prevCluster != null && prevCluster.version() > (clusterInformation.version())) {
                return false;
            } else if (clusterInformationAtomicReference.compareAndSet(prevCluster, clusterInformation)) {
                LOGGER.info("Cluster info was update to: {}", clusterInformation);
                return true;
            }
        }
    }

    /**
     * Get current node status
     *
     * @return NodeStatus
     */

    protected NodeStatus getNodeStatus() {
        return nodeStatus.get();
    }


    private ExecutorService getServerPool() {
        return serverPool;
    }
}
