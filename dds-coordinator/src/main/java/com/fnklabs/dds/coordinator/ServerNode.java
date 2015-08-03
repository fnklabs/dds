package com.fnklabs.dds.coordinator;

import com.fnklabs.dds.coordinator.exception.NodeIsNodeAvailableForOperations;
import com.fnklabs.dds.coordinator.partition.PartitionTable;
import com.fnklabs.dds.coordinator.partition.Partitioner;
import com.fnklabs.dds.coordinator.partition.exception.RepartitionIllegalOperation;
import com.fnklabs.dds.network.ApiVersion;
import com.fnklabs.dds.network.server.NetworkServer;
import com.google.common.collect.Sets;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Server node implementation
 */
class ServerNode implements Node {
    /**
     * Logger
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(ServerNode.class);

    /**
     * Network server instance
     */
    @NotNull
    private final NetworkServer networkServer;

    /**
     * Executor service
     */
    @NotNull
    private final ExecutorService executorService;

    /**
     * Is running status
     */
    @NotNull
    private final AtomicBoolean isRunning = new AtomicBoolean(false);

    /**
     * Atomic reference to last actual node status
     */
    private final AtomicReference<NodeStatus> nodeStatus = new AtomicReference<>(NodeStatus.START_UP);

    /**
     *
     */
    @NotNull
    private final Configuration configuration;

    /**
     *
     */
    @NotNull
    private final ServerNodeClientFactory serverNodeClientFactory;

    /**
     * Cluster information reference
     */
    private final AtomicReference<ClusterInformation> clusterInformationAtomicReference = new AtomicReference<>();

    /**
     * Construct new local node
     *
     * @param listenAddressAndPort    Local node address on which server will bind
     * @param configuration           Configuration holder
     * @param executorService         Executor service
     * @param serverNodeClientFactory ServerNodeClient factory
     */
    protected ServerNode(@NotNull HostAndPort listenAddressAndPort,
                         @NotNull Configuration configuration,
                         @NotNull ExecutorService executorService,
                         @NotNull ServerNodeClientFactory serverNodeClientFactory) {
        this.executorService = executorService;
        this.configuration = configuration;
        this.serverNodeClientFactory = serverNodeClientFactory;
        networkServer = NetworkServer.create(listenAddressAndPort, new ServerMessageHandler(this, executorService));
    }

    /**
     * Start server node
     */
    public void start() {
        isRunning.set(true);

        getExecutorService().submit(new WatchDog(this, isRunning, getExecutorService()));
    }

    @NotNull
    @Override
    public HostAndPort getAddressAndPort() {
        return networkServer.getListenAddress();
    }

    /**
     * Return information about current server node
     *
     * @return NodeInfo
     */
    @Override
    public ListenableFuture<NodeInfo> getNodeInfo() {
        SettableFuture<NodeInfo> nodeInfoSettableFuture = SettableFuture.<NodeInfo>create();
        nodeInfoSettableFuture.set(getCurrentNodeInfo());
        return nodeInfoSettableFuture;
    }

    /**
     * Get cluster information
     *
     * @return RingInfo
     */
    @Override
    public ListenableFuture<ClusterInformation> getClusterInfo() {
        SettableFuture<ClusterInformation> clusterInformationSettableFuture = SettableFuture.<ClusterInformation>create();


        if (ifAvailableForOperations()) {
            clusterInformationSettableFuture.set(clusterInformationAtomicReference.get());

            return clusterInformationSettableFuture;
        }

        clusterInformationSettableFuture.setException(new NodeIsNodeAvailableForOperations());
        return clusterInformationSettableFuture;
    }


    @Override
    public ListenableFuture<ClusterInformation> nodeUp(NodeInfo nodeInfo) {
        LOGGER.info("Node up: {}", nodeInfo);

        ListenableFuture<ClusterInformation> clusterInfoFuture = getClusterInfo();


        return Futures.transform(clusterInfoFuture, (ClusterInformation input) -> {
            TreeSet<NodeInfo> members = new TreeSet<>(input.getMembers());
            members.add(this.getCurrentNodeInfo());


            if (!input.getMembers().contains(nodeInfo)) {
                nodeStatus.set(NodeStatus.REPAIR);
            }

            ClusterStatus clusterStatus = isSynchronizing() ? ClusterStatus.INCONSISTENT : input.getClusterStatus();

            ClusterInformation clusterInformation = new ClusterInformation(input.getCoordinator(), members, getCurrentNodeInfo(), clusterStatus, input.getPartitionTable());

            List<ListenableFuture<Boolean>> collect = input
                    .getMembers()
                    .stream()
                    .map(item -> serverNodeClientFactory.getInstance(item.getAddress()).repair(clusterInformation))
                    .collect(Collectors.toList());

            ListenableFuture<List<Boolean>> listListenableFuture = Futures.allAsList(collect);

            Futures.addCallback(listListenableFuture, new FutureCallback<List<Boolean>>() {
                @Override
                public void onSuccess(List<Boolean> result) {
                    LOGGER.info("Repair operation result: {}", result);
                }

                @Override
                public void onFailure(Throwable t) {
                    LOGGER.warn("Cant repair cluster", t);
                }
            }, getExecutorService());

            return Futures.transform(updateClusterInfo(clusterInformation), (Boolean updateStatus) -> {
                return clusterInformation;
            }, getExecutorService());

        }, getExecutorService());
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
    public ListenableFuture<Boolean> nodeDown(NodeInfo nodeInfo) {
        LOGGER.info("Node down: {}", nodeInfo);

        return Futures.transform(getClusterInfo(), (ClusterInformation input) -> {
            TreeSet<NodeInfo> members = new TreeSet<>(input.getMembers());
            members.remove(nodeInfo);

            ClusterInformation clusterInformation = new ClusterInformation(input.getCoordinator(), members, getCurrentNodeInfo(), input.getClusterStatus(), input.getPartitionTable());

            return updateClusterInfo(clusterInformation);
        }, getExecutorService());

    }

    @Override
    public ListenableFuture<Boolean> repair(ClusterInformation clusterInformation) {
        ListenableFuture<Boolean> resultFuture = updateClusterInfo(clusterInformation);

        ListenableFuture<Boolean> transform = Futures.transform(resultFuture, (Boolean result) -> {
            if (result) {
                nodeStatus.set(NodeStatus.REPAIR);
            }

            return true;
        }, getExecutorService());

        Futures.addCallback(transform, new FutureCallback<Boolean>() {
            @Override
            public void onSuccess(Boolean result) {

            }

            @Override
            public void onFailure(Throwable t) {
                LOGGER.warn("Cant update cluster info", t);
            }
        }, getExecutorService());

        return transform;
    }

    @Override
    public ListenableFuture<Boolean> updateClusterInfo(ClusterInformation clusterInformation) {
        SettableFuture<Boolean> resultFuture = SettableFuture.<Boolean>create();

        for (; ; ) {
            ClusterInformation prevClusterInformation = clusterInformationAtomicReference.get();

            if (prevClusterInformation != null && prevClusterInformation.getCreated().isAfter(clusterInformation.getCreated())) {
                resultFuture.set(false);
                break;
            } else if (clusterInformationAtomicReference.compareAndSet(prevClusterInformation, clusterInformation)) {
                LOGGER.info("Cluster info was update to: {}", clusterInformation);
                resultFuture.set(true);
                break;
            }
        }

        return resultFuture;
    }

    @Override
    public ListenableFuture<Long> ping() {
        SettableFuture<Long> pingFuture = SettableFuture.<Long>create();
        pingFuture.set(0l);
        return pingFuture;
    }

    @Override
    public void close() {
        LOGGER.warn("Shutting down node...");

        networkServer.close();

        LOGGER.warn("Node was shutdown");
    }

    /**
     * Get current node status
     *
     * @return
     */
    protected NodeStatus getNodeStatus() {
        return nodeStatus.get();
    }

    protected ClusterStatus getClusterStatus() {
        return clusterInformationAtomicReference.get().getClusterStatus();
    }

    /**
     * On repair status
     */
    protected void onRepair() {
        // run repair operation
    }

    /**
     * Setup current node into cluster
     */
    protected void onSetUp() {
        LOGGER.info("Try to setup node");

        if (updateNodeStatus(NodeStatus.SETUP, NodeStatus.SETTING_UP)) {
            Set<HostAndPort> seeds = getConfiguration().getSeeds();

            HashSet<HostAndPort> seedSet = new HashSet<>(seeds);

            if (seedSet.isEmpty()) {
                setUp();
            } else {
                setUpInTheCluster(seedSet);
            }

        } else {
            LOGGER.warn("Cant setup server, invalid current status: {}", getNodeStatus());
        }
    }

    protected void onStartUp() {
        LOGGER.info("Node status is {} StartUp server...", nodeStatus);

        if (updateNodeStatus(NodeStatus.START_UP, NodeStatus.STARTING_UP)) {
            updateNodeStatus(NodeStatus.STARTING_UP, NodeStatus.SETUP);
        } else {
            LOGGER.warn("Cant start up node");
        }
    }

    private boolean isSynchronizing() {
        return getNodeStatus() == NodeStatus.REPAIR || getNodeStatus() == NodeStatus.SYNCHRONIZATION;
    }

    private boolean ifAvailableForOperations() {
        NodeStatus nodeStatus = this.nodeStatus.get();

        return nodeStatus == NodeStatus.UP || nodeStatus == NodeStatus.REPAIR || nodeStatus == NodeStatus.SYNCHRONIZATION;
    }

    private void setUp() {
        TreeSet<UUID> members = new TreeSet<>(Sets.newHashSet(getCurrentNodeInfo().getId()));

        try {
            PartitionTable partitionTable = Partitioner.buildPartitionTable(members, configuration.getReplicationFactor());

            ClusterInformation clusterInformation = new ClusterInformation(getCurrentNodeInfo(),
                    new TreeSet<>(Sets.newHashSet(getCurrentNodeInfo())),
                    getCurrentNodeInfo(),
                    ClusterStatus.UNKNOWN,
                    partitionTable);

            updateClusterInfo(clusterInformation);
        } catch (RepartitionIllegalOperation repartitionIllegalOperation) {
            LOGGER.warn("Cant build partition table", repartitionIllegalOperation);
        }


    }

    /**
     * SetUp current node in cluster using one of the specified seeds
     *
     * @param seeds Cluster seed which can be used to retrieve cluster information {@link Node#getClusterInfo()} and set up in the cluster {@link Node#nodeUp(NodeInfo)}
     */
    private void setUpInTheCluster(final HashSet<HostAndPort> seeds) {
        Optional<HostAndPort> first = seeds.stream().findFirst();

        if (!first.isPresent()) {
            LOGGER.warn("Seems server is isolated from cluster (no available seeds). Let's shutdown server");

            updateNodeStatus(NodeStatus.SETTING_UP, NodeStatus.SHUTDOWN);
        } else {
            HostAndPort nodeAddress = first.get();

            ServerNodeClient serverClient = getServerNodeClientInstance(nodeAddress);

            ListenableFuture<ClusterInformation> setUpFuture = serverClient.nodeUp(getCurrentNodeInfo());

            Futures.addCallback(setUpFuture, new FutureCallback<ClusterInformation>() {
                @Override
                public void onSuccess(ClusterInformation result) {

                    updateClusterInfo(result);

                    Futures.addCallback(updateClusterInfo(result), new FutureCallback<Boolean>() {
                        @Override
                        public void onSuccess(Boolean result) {
                            LOGGER.info("Cluster info was successfully updated");

                            updateNodeStatus(NodeStatus.SETTING_UP, NodeStatus.UP);
                        }

                        @Override
                        public void onFailure(Throwable t) {
                            LOGGER.warn("Can't update cluster info. Shutdown server");

                            updateNodeStatus(NodeStatus.SETTING_UP, NodeStatus.SHUTDOWN);
                        }
                    }, getExecutorService());
                }

                @Override
                public void onFailure(Throwable t) {
                    LOGGER.warn("Cant set up node. Shutdown server, because error occurred", t);

                    seeds.remove(first.get());

                    setUpInTheCluster(seeds);
                }
            }, getExecutorService());


        }
    }

    private ServerNodeClient getServerNodeClientInstance(HostAndPort nodeAddress) {
        return serverNodeClientFactory.getInstance(nodeAddress);
    }

    @NotNull
    private Configuration getConfiguration() {
        return configuration;
    }

    private boolean updateNodeStatus(NodeStatus expectedValue, NodeStatus newValue) {
        return nodeStatus.compareAndSet(expectedValue, newValue);
    }

    @NotNull
    private NodeInfo getCurrentNodeInfo() {
        return new NodeInfo(configuration.getNodeId(), getAddressAndPort(), ApiVersion.VERSION_1, nodeStatus.get());
    }

    @NotNull
    private ExecutorService getExecutorService() {
        return executorService;
    }
}
