package com.fnklabs.dds.cluster;

import com.fnklabs.concurrent.Executors;
import com.fnklabs.dds.cluster.partition.PartitionTable;
import com.fnklabs.dds.cluster.partition.Partitioner;
import com.fnklabs.dds.cluster.partition.exception.RepartitionIllegalOperation;
import com.fnklabs.dds.network.pool.NetworkExecutor;
import com.fnklabs.dds.network.pool.NioExecutor;
import com.fnklabs.dds.network.server.NetworkServer;
import com.google.common.collect.Sets;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.jetbrains.annotations.Nullable;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Server node implementation
 */
class NodeImpl implements Node {
    private final UUID id = UUID.randomUUID();

    /**
     * Logger
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(NodeImpl.class);

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
    private final AtomicReference<NodeStatus> nodeStatus = new AtomicReference<>(NodeStatus.START_UP);

    /**
     *
     */

    private final ServerNodeClientFactory serverNodeClientFactory;

    /**
     * Cluster information reference
     */
    private final AtomicReference<Cluster> clusterInformationAtomicReference = new AtomicReference<>();

    private final NetworkExecutor networkExecutor;

    /**
     * Construct new local node
     *
     * @param listenAddressAndPort    Local node address on which server will bind
     * @param workers                 Executor service
     * @param serverNodeClientFactory ServerNodeClient factory
     */
    NodeImpl(Configuration configuration, ServerNodeClientFactory serverNodeClientFactory) throws IOException {

        this.serverPool = Executors.fixedPoolExecutor(configuration.getWorkerPoolSize(), "dds.server.worker");
        this.serverNodeClientFactory = serverNodeClientFactory;

        this.networkExecutor = NioExecutor.builder()
                                          .setOpAcceptExecutor(configuration.getNioPoolSize())
                                          .setOpReadExecutor(configuration.getNioPoolSize())
                                          .setOpWriteExecutor(configuration.getNioPoolSize())
                                          .build();

        this.networkServer = new NetworkServer(configuration.listenAddress(), configuration.getNetworkPoolSize(), new ServerIncomeMessageHandler(this));

        for (int i = 0; i < configuration.getWorkerPoolSize(); i++) {
            getServerPool().submit(new WatchDog(this, isRunning));
        }
    }

    /**
     * Start server node
     */
    public void start() throws IOException {
        isRunning.set(true);
        networkExecutor.run();

        networkServer.join(networkExecutor);
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
        return null;
    }

    @Override
    public NodeStatus getStatus() {
        return null;
    }

    @Override
    public DateTime getLastUpdated() {
        return null;
    }

    @Override
    public HostAndPort getAddress() {
        return null;
    }

    @Override
    public Version getVersion() {
        return null;
    }

    @Override
    public Configuration getConfiguration() {
        return null;
    }

    @Override
    public HostAndPort getAddressAndPort() {
        return null;//networkServer.getListenAddress();
    }

    /**
     * Notify server that specified node was up or can be used to update information about node instead of using {@link
     * NodeImpl#updateClusterInfo(Cluster)}  to update information about whole cluster.
     *
     * @param nodeInfo New node info
     *
     * @return new Cluster
     */
    @Override
    public ListenableFuture<Cluster> nodeUp(Node nodeInfo) {
        LOGGER.info("Node up: {}", nodeInfo);

        ListenableFuture<Cluster> clusterInfoFuture = getClusterInfo();


        return Futures.transform(clusterInfoFuture, (Cluster input) -> {
//            NodeInfo currentNodeInfo = getCurrentNodeInfo();
//
//            TreeSet<NodeInfo> members = new TreeSet<>();
//            members.addAll(input.getMembers());
//            members.remove(currentNodeInfo);
//            members.add(currentNodeInfo);
//            members.add(nodeInfo);
//
//            Cluster clusterInformation = new Cluster(members, currentNodeInfo, input.getPartitionTable());
//
//            return Futures.transform(updateClusterInfo(clusterInformation), new com.google.common.base.Function<Boolean, Cluster>() {
//                @Override
//                public Cluster apply(Boolean updateStatus) {
//                    if (updateStatus && !input.getMembers().contains(nodeInfo)) {
//                        for (; ; ) {
//                            NodeStatus nodeStatus = NodeImpl.this.nodeStatus.get();
//                            if (updateNodeStatus(nodeStatus, NodeStatus.REPAIR)) {
//                                break;
//                            }
//                        }
//                    }
//
//                    return clusterInformation;
//                }
//            });

            return null;

        });
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

        return Futures.transform(getClusterInfo(), (Cluster input) -> {
//            TreeSet<NodeInfo> members = new TreeSet<>();
//            members.addAll(input.getMembers());
//            members.remove(nodeInfo);
//
//            Cluster clusterInformation = new Cluster(members, getCurrentNodeInfo(), input.getPartitionTable());
//
//            return updateClusterInfo(clusterInformation);
            return null;
        }, getServerPool());

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
        ListenableFuture<Boolean> resultFuture = updateClusterInfo(clusterInformation);

        return Futures.transform(resultFuture, (Boolean result) -> {
            if (result) {
                nodeStatus.set(NodeStatus.REPAIR);
            }

            return true;
        }, getServerPool());
    }

    @Override
    public ListenableFuture<Node> getNode() {
        return null;
    }

    /**
     * Get cluster information
     *
     * @return RingInfo
     */
    @Override
    public ListenableFuture<Cluster> getClusterInfo() {
        SettableFuture<Cluster> clusterInformationSettableFuture = SettableFuture.<Cluster>create();
        clusterInformationSettableFuture.set(getCluster());

        return clusterInformationSettableFuture;
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


    protected ClusterStatus getClusterStatus() {
        Cluster clusterInformation = getCluster();
        return clusterInformation == null ? ClusterStatus.UNKNOWN : clusterInformation.getClusterStatus();
    }


    private boolean isSynchronizing() {
        return getNodeStatus() == NodeStatus.REPAIR || getNodeStatus() == NodeStatus.SYNCHRONIZATION;
    }

    private boolean ifAvailableForOperations() {
        NodeStatus nodeStatus = this.nodeStatus.get();

        return nodeStatus == NodeStatus.UP
                || nodeStatus == NodeStatus.REPAIR
                || nodeStatus == NodeStatus.SYNCHRONIZATION
                || nodeStatus == NodeStatus.DOWN
                || nodeStatus == NodeStatus.SHUTDOWN;
    }

    private void setUp() {
        TreeSet<Node> members = new TreeSet<>(Sets.newHashSet(getCurrentNodeInfo()));

        try {
            PartitionTable partitionTable = Partitioner.buildPartitionTable(members, getConfiguration().replicationFactor());

            Cluster clusterInformation = new ClusterImpl();

            updateClusterInfo(clusterInformation);
        } catch (RepartitionIllegalOperation repartitionIllegalOperation) {
            LOGGER.warn("Cant build partition table", repartitionIllegalOperation);
        }


    }


    private Node getServerNodeClientInstance(HostAndPort nodeAddress) {
        return serverNodeClientFactory.getInstance(nodeAddress);
    }


    private Node getCurrentNodeInfo() {
        return null;//new NodeInfo(configuration.getNodeId(), getAddressAndPort(), ApiVersion.VERSION_1, nodeStatus.get());
    }


    private ExecutorService getServerPool() {
        return serverPool;
    }
}
