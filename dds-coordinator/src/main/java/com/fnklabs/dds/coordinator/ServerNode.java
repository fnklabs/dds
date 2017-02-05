package com.fnklabs.dds.coordinator;

import com.fnklabs.dds.coordinator.partition.PartitionTable;
import com.fnklabs.dds.coordinator.partition.Partitioner;
import com.fnklabs.dds.coordinator.partition.exception.RepartitionIllegalOperation;
import com.fnklabs.dds.network.NetworkServer;
import com.google.common.collect.Sets;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
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
        networkServer = null;//NetworkServer.create(listenAddressAndPort, new ServerIncomeMessageHandler(this, executorService));
    }

    /**
     * Start server node
     */
    public void start() {
        isRunning.set(true);

        getExecutorService().submit(new WatchDog(this, isRunning, getExecutorService()));
    }

    @Override
    public HostAndPort getAddressAndPort() {
        return null;//networkServer.getListenAddress();
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
        clusterInformationSettableFuture.set(getClusterInformation());

        return clusterInformationSettableFuture;
    }

    @Nullable
    public ClusterInformation getClusterInformation() {
        return clusterInformationAtomicReference.get();
    }


    /**
     * Notify server that specified node was up or can be used to update information about node instead of using {@link
     * ServerNode#updateClusterInfo(ClusterInformation)}  to update information about whole cluster.
     *
     * @param nodeInfo New node info
     *
     * @return new ClusterInformation
     */
    @Override
    public ListenableFuture<ClusterInformation> nodeUp(NodeInfo nodeInfo) {
        LOGGER.info("Node up: {}", nodeInfo);

        ListenableFuture<ClusterInformation> clusterInfoFuture = getClusterInfo();


        return Futures.transform(clusterInfoFuture, (ClusterInformation input) -> {
//            NodeInfo currentNodeInfo = getCurrentNodeInfo();
//
//            TreeSet<NodeInfo> members = new TreeSet<>();
//            members.addAll(input.getMembers());
//            members.remove(currentNodeInfo);
//            members.add(currentNodeInfo);
//            members.add(nodeInfo);
//
//            ClusterInformation clusterInformation = new ClusterInformation(members, currentNodeInfo, input.getPartitionTable());
//
//            return Futures.transform(updateClusterInfo(clusterInformation), new com.google.common.base.Function<Boolean, ClusterInformation>() {
//                @Override
//                public ClusterInformation apply(Boolean updateStatus) {
//                    if (updateStatus && !input.getMembers().contains(nodeInfo)) {
//                        for (; ; ) {
//                            NodeStatus nodeStatus = ServerNode.this.nodeStatus.get();
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
    public ListenableFuture<Boolean> nodeDown(NodeInfo nodeInfo) {
        LOGGER.info("Node down: {}", nodeInfo);

        return Futures.transform(getClusterInfo(), (ClusterInformation input) -> {
//            TreeSet<NodeInfo> members = new TreeSet<>();
//            members.addAll(input.getMembers());
//            members.remove(nodeInfo);
//
//            ClusterInformation clusterInformation = new ClusterInformation(members, getCurrentNodeInfo(), input.getPartitionTable());
//
//            return updateClusterInfo(clusterInformation);
            return null;
        }, getExecutorService());

    }

    /**
     * Update node status to start repair operation
     *
     * @param clusterInformation New cluster information by which repartition must be done
     *
     * @return Future for accepting operation
     */
    @Override
    public ListenableFuture<Boolean> repair(ClusterInformation clusterInformation) {
        ListenableFuture<Boolean> resultFuture = updateClusterInfo(clusterInformation);

        return Futures.transform(resultFuture, (Boolean result) -> {
            if (result) {
                nodeStatus.set(NodeStatus.REPAIR);
            }

            return true;
        }, getExecutorService());
    }

    @Override
    public ListenableFuture<Boolean> updateClusterInfo(ClusterInformation clusterInformation) {
        return Futures.immediateFuture(updateClusterInformation(clusterInformation));
    }

    public boolean updateClusterInformation(ClusterInformation clusterInformation) {
        for (; ; ) {
            ClusterInformation prevClusterInformation = clusterInformationAtomicReference.get();

            if (prevClusterInformation != null && prevClusterInformation.getCreated().isAfter(clusterInformation.getCreated())) {
                return false;
            } else if (clusterInformationAtomicReference.compareAndSet(prevClusterInformation, clusterInformation)) {
                LOGGER.info("Cluster info was update to: {}", clusterInformation);
                return true;
            }
        }
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

    }

    /**
     * Get current node status
     *
     * @return NodeStatus
     */
    @NotNull
    protected NodeStatus getNodeStatus() {
        return nodeStatus.get();
    }

    @NotNull
    protected ClusterStatus getClusterStatus() {
        ClusterInformation clusterInformation = getClusterInformation();
        return clusterInformation == null ? ClusterStatus.UNKNOWN : clusterInformation.getClusterStatus();
    }

    /**
     * Execute repair operation. Called by watchdog on node status {@link com.fnklabs.dds.coordinator.Node.NodeStatus#REPAIR}
     * <p>
     * At first will change node status to {@link com.fnklabs.dds.coordinator.Node.NodeStatus#SYNCHRONIZATION} than retrieve current cluster information and
     * recalculate partition table. After that it would create new cluster information and if repair initiator was current node than it must send notification
     * to all members except current and run {@link #repair(PartitionTable)} method to start repair operation in current node
     * <p>
     * If {@link #repair(PartitionTable)} will be completed successfully change node status to {@link com.fnklabs.dds.coordinator.Node.NodeStatus#UP} status
     */
    protected ListenableFuture<Boolean> onRepair() {
        // run repair operation

        if (!updateNodeStatus(NodeStatus.REPAIR, NodeStatus.SYNCHRONIZATION)) {
            LOGGER.warn("Cant update node status from REPAIR to SYNCHRONIZATION");

            SettableFuture<Boolean> responseFuture = SettableFuture.<Boolean>create();
            responseFuture.set(false);
            return responseFuture;
        }

        ListenableFuture<Boolean> repairFuture = Futures.transform(getClusterInfo(), (ClusterInformation result) -> {
            TreeSet<UUID> members = new TreeSet<>(result.getMembers().stream().map(NodeInfo::getId).collect(Collectors.toSet()));

            PartitionTable partitionTable = Partitioner.buildPartitionTable(members, getConfiguration().getReplicationFactor());

            ClusterInformation clusterInformation = new ClusterInformation(result.getMembers(), getCurrentNodeInfo(), partitionTable);

            ListenableFuture<Boolean> updateClusterInfoFuture = updateClusterInfo(clusterInformation);

//            return Futures.transform(updateClusterInfoFuture, (Boolean input) -> {
//
////                if (result.getSender()
////                          .equals(getCurrentNodeInfo())) { // if we was initiator of repair operation than send notification to all members except us
////
////                    List<ListenableFuture<Boolean>> collect = result.getMembers()
////                                                                    .stream()
////                                                                    .filter(nodeInfo -> !nodeInfo.getAddress().equals(getAddressAndPort()))
////                                                                    .map(nodeInfo -> sendMessage(nodeInfo, service -> service.repair(clusterInformation)))
////                                                                    .collect(Collectors.toList());
////
////                    ListenableFuture<List<Boolean>> future = Futures.allAsList(collect);
////
////                    Futures.addCallback(future, new FutureCallback<List<Boolean>>() {
////                        @Override
////                        public void onSuccess(List<Boolean> result) {
////                            LOGGER.warn("Repair operation was send: {}", result);
////                        }
////
////                        @Override
////                        public void onFailure(Throwable t) {
////                            LOGGER.warn("Can't send repair operations", t);
////                        }
////                    }, getExecutorService());
////                }
////
////                return repair(partitionTable);
//
//                return null;
//            }, getExecutorService());

            return null;
        }, getExecutorService());

        Futures.addCallback(repairFuture, new FutureCallback<Boolean>() {
            @Override
            public void onSuccess(Boolean result) {
                LOGGER.info("Repair operation completed with status: {}", result);

                if (result) {
                    updateNodeStatus(NodeStatus.SYNCHRONIZATION, NodeStatus.UP);

                } else {
                    LOGGER.warn("Cant complete repair operation. Restarting repair operation");
                    updateNodeStatus(NodeStatus.SYNCHRONIZATION, NodeStatus.REPAIR);
                }
            }

            @Override
            public void onFailure(Throwable t) {
                LOGGER.warn("Cant complete repair operation. Restarting repair operation", t);
                updateNodeStatus(NodeStatus.SYNCHRONIZATION, NodeStatus.REPAIR);
            }
        });

        return repairFuture;
    }

    /**
     * Must be overridden to realize repair operation
     *
     * @param partitionTable new partition table
     *
     * @return Future for repair operation, must return true if all is ok, false otherwise
     */
    protected ListenableFuture<Boolean> repair(PartitionTable partitionTable) {
        SettableFuture<Boolean> responseFuture = SettableFuture.<Boolean>create();
        responseFuture.set(true);
        return responseFuture;
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

        return nodeStatus == NodeStatus.UP
                || nodeStatus == NodeStatus.REPAIR
                || nodeStatus == NodeStatus.SYNCHRONIZATION
                || nodeStatus == NodeStatus.DOWN
                || nodeStatus == NodeStatus.SHUTDOWN;
    }

    private void setUp() {
        TreeSet<UUID> members = new TreeSet<>(Sets.newHashSet(getCurrentNodeInfo().getId()));

        try {
            PartitionTable partitionTable = Partitioner.buildPartitionTable(members, configuration.getReplicationFactor());

            ClusterInformation clusterInformation = new ClusterInformation(
                    new TreeSet<>(Sets.newHashSet(getCurrentNodeInfo())),
                    getCurrentNodeInfo(),
                    partitionTable
            );

            updateClusterInfo(clusterInformation);
        } catch (RepartitionIllegalOperation repartitionIllegalOperation) {
            LOGGER.warn("Cant build partition table", repartitionIllegalOperation);
        }


    }

    /**
     * SetUp current node in cluster using one of the specified seeds
     *
     * @param seeds Cluster seed which can be used to retrieve cluster information {@link Node#getClusterInfo()} and set up in the cluster {@link
     *              Node#nodeUp(NodeInfo)}
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

    /**
     * Change current node status
     *
     * @param expectedValue Expected node status
     * @param newValue      New node status
     *
     * @return True if node status was updated, False otherwise
     */
    private boolean updateNodeStatus(NodeStatus expectedValue, NodeStatus newValue) {
        LOGGER.warn("Update current node status {} to {}", expectedValue, newValue);
        boolean updateResult = nodeStatus.compareAndSet(expectedValue, newValue);

        if (updateResult) {

            Futures.addCallback(sendOurNodeInfo(), new FutureCallback<Boolean>() {
                @Override
                public void onSuccess(Boolean result) {
                    LOGGER.warn("Complete send node info with status: {}", result);
                }

                @Override
                public void onFailure(Throwable t) {
                    LOGGER.warn("Cant send node info. Exception stack trace:", t);
                }
            }, getExecutorService());


            ClusterInformation clusterInformation = getClusterInformation();

            if (clusterInformation != null) {

                NodeInfo currentNodeInfo = getCurrentNodeInfo();

                LOGGER.debug("Current node info: {}", currentNodeInfo);

                TreeSet<NodeInfo> members = new TreeSet<>();
                members.addAll(clusterInformation.getMembers());
                members.remove(currentNodeInfo);
                members.add(currentNodeInfo);

                ClusterInformation newClusterInfo = new ClusterInformation(members, currentNodeInfo, clusterInformation.getPartitionTable());
                return updateClusterInformation(newClusterInfo);
            } else {
                LOGGER.warn("Cant update cluster information because current value is null. Seems ser start up operation in progress. Current node status: {}", getNodeStatus());
            }

        }
        return updateResult;
    }

    /**
     * Send our node info to all members in the cluster
     *
     * @return Future operation. True if operation was successfully completed False otherwise
     */
    private ListenableFuture<Boolean> sendOurNodeInfo() {

        ClusterInformation clusterInformation = getClusterInformation();

        if (clusterInformation == null) {
            LOGGER.warn("Cluster information is null, seems server setup operation in progress. Server status: {}", getClusterStatus());

            SettableFuture<Boolean> responseFuture = SettableFuture.<Boolean>create();
            responseFuture.set(false);
            return responseFuture;
        }

        List<ListenableFuture<ClusterInformation>> collect = clusterInformation.getMembers()
                                                                               .stream()
                                                                               .filter(nodeInfo -> !nodeInfo.getAddress().equals(getAddressAndPort()))
                                                                               .map(nodeInfo -> sendMessage(nodeInfo, service -> service.nodeUp(getCurrentNodeInfo())))
                                                                               .collect(Collectors.toList());

        return Futures.transform(Futures.allAsList(collect), (List<ClusterInformation> input) -> {
            return input.stream().allMatch(item -> item != null);
        }, getExecutorService());

    }

    /**
     * Send message to remote node
     * All messages must be send via current method, because it update node status
     *
     * @param nodeInfo        Remote node info
     * @param serviceFunction Function for execution message from service
     * @param <T>             Response class type
     *
     * @return Future for response
     */
    private <T> ListenableFuture<T> sendMessage(NodeInfo nodeInfo, Function<Node, ListenableFuture<T>> serviceFunction) {
        ServerNodeClient serverNodeClientInstance = getServerNodeClientInstance(nodeInfo.getAddress());

        ListenableFuture<T> apply = serviceFunction.apply(serverNodeClientInstance);

        Futures.addCallback(apply, new FutureCallback<T>() {
            @Override
            public void onSuccess(T result) {
                LOGGER.debug("Message was successfully send to {} and retrieved response {}", nodeInfo, result);
            }

            @Override
            public void onFailure(Throwable t) {
                LOGGER.warn("Can't send to remote node: " + nodeInfo.toString(), t);

                updateNodeInfoStatus(nodeInfo, NodeStatus.NOT_RESPOND);
            }
        }, getExecutorService());

        return apply;
    }

    /**
     * Update specified node info status and update cluster information
     *
     * @param nodeInfo   NodeInfo which will be updated
     * @param nodeStatus new node info status
     */
    private void updateNodeInfoStatus(NodeInfo nodeInfo, NodeStatus nodeStatus) {
        ClusterInformation clusterInformation = getClusterInformation();

        TreeSet<NodeInfo> members = new TreeSet<>(clusterInformation.getMembers());
        members.remove(nodeInfo);
        members.add(new NodeInfo(nodeInfo.getId(), nodeInfo.getAddress(), nodeInfo.getVersion(), nodeStatus));


        ClusterInformation newClusterInformation = new ClusterInformation(members, getCurrentNodeInfo(), clusterInformation.getPartitionTable());

        updateClusterInformation(newClusterInformation);

        LOGGER.debug("Node and cluster information was updated: {}", getClusterInformation());
    }

    @NotNull
    private NodeInfo getCurrentNodeInfo() {
        return null;//new NodeInfo(configuration.getNodeId(), getAddressAndPort(), ApiVersion.VERSION_1, nodeStatus.get());
    }

    @NotNull
    private ExecutorService getExecutorService() {
        return executorService;
    }
}
