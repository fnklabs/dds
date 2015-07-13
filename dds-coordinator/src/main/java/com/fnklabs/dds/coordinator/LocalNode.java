package com.fnklabs.dds.coordinator;

import com.fnklabs.dds.Metrics;
import com.fnklabs.dds.network.Server;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.*;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Local node implementation
 */
public class LocalNode implements Node {
    /**
     * Logger
     */
    public static final Logger LOGGER = LoggerFactory.getLogger(Node.class);
    /**
     * Local data dir
     */
    public static final String DATA_DIR = "data";
    /**
     *
     */
    public static final int CHUNK_INPUT_BUFFER = 512 * 1024; // 512 Kb
    /**
     * Data directory
     */
    private static final File DATA_DIRECTORY = new File(DATA_DIR);
    /**
     *
     */
    private static ConcurrentHashMap<UUID, ObjectOutputStream> chunksOutputStreams = new ConcurrentHashMap<>();

    /**
     * Local node address
     */
    private HostAndPort address;
    /**
     * Server instance
     */
    @Deprecated
    private Server server;
    /**
     * Ring information
     */
    private Ring ring;
    /**
     * Executor service
     */
    private ListeningExecutorService executorService;
    /**
     * Node factory
     */
    private NodeFactory nodeFactory;

    /**
     * Construct new local node
     *
     * @param address          Local node address
     * @param executorService  Executor service
     * @param scheduledService Scheduler service
     * @param nodeFactory      NodeFactory
     *
     * @throws IOException
     */
    public LocalNode(HostAndPort address, ListeningExecutorService executorService, ListeningScheduledExecutorService scheduledService, NodeFactory nodeFactory) throws IOException {
        this.address = address;
        this.executorService = executorService;
        this.nodeFactory = nodeFactory;

//        server = Server.create(address.getHostText(), address.getPortOrDefault(DEFAULT_PORT));
//        server.register(new ServerEventsHandler(this));
//        server.start();


        scheduledService.scheduleWithFixedDelay(new WatchDog(this), 1000, 100, TimeUnit.MILLISECONDS);
        scheduledService.scheduleWithFixedDelay(() -> {
            if (getRing() != null && getRing().getRingStatus() == RingStatus.RUNNING) {
                ConfigurationStore.update(getRing().getRingInfo());
            }
        }, 1000, 100, TimeUnit.MILLISECONDS);
    }

    @Override
    public HostAndPort getAddress() {
        return address;
    }

    @Override
    public NodeInfo getNodeInfo() {
        return new NodeInfo(getAddress(), getNodeVersion());
    }

    @Override
    public <T extends Record> ListenableFuture<Boolean> destroyDataSet(DistributedDataSet<T> distributedDataSet) {
        SettableFuture<Boolean> responseFuture = SettableFuture.<Boolean>create();

        getExecutorService().submit(() -> {
            AtomicBoolean hasErrors = new AtomicBoolean(false);

            List<ListenableFuture<Boolean>> resultList = new ArrayList<>();

            distributedDataSet.getChunks().forEach(chunk -> resultList.add(destroyChunk(chunk)));

            ListenableFuture<List<Boolean>> resultListFuture = Futures.allAsList(resultList);

            Futures.addCallback(resultListFuture, new FutureCallback<List<Boolean>>() {
                @Override
                public void onSuccess(List<Boolean> result) {
                    long failedItems = result.stream().filter(item -> !item).count();

                    if (failedItems == 0) {
                        try {
                            FileUtils.deleteDirectory(getDataSetDir(distributedDataSet.getName()));

                            hasErrors.set(false);
                            return;
                        } catch (IOException e) {
                            LOGGER.warn("Cant delete dds dir: " + getDataSetDir(distributedDataSet.getName()), e);
                        }
                    }

                    hasErrors.set(true);
                }

                @Override
                public void onFailure(Throwable t) {
                    hasErrors.set(true);

                    LOGGER.warn("Cant remove destroy chunks", t);
                }
            }, getExecutorService());

            responseFuture.set(hasErrors.get());
        });

        return responseFuture;
    }

    @Override
    public <T extends Record> ListenableFuture<List<ChunkDataSet<T>>> discoverChunks(DistributedDataSet<T> distributedDataSet) {

        SettableFuture<List<ChunkDataSet<T>>> responseFuture = SettableFuture.<List<ChunkDataSet<T>>>create();

        getExecutorService().submit(() -> {
            File dataSetDir = getDataSetDir(distributedDataSet.getName());

            List<ChunkDataSet<T>> chunks = new ArrayList<>();

            File[] files = dataSetDir.listFiles();

            if (files != null) {
                for (File chunk : files) {
                    LoggerFactory.getLogger(getClass()).debug("Discovering chunk: {}", chunk.getName());

                    chunks.add(new ChunkDataSet<T>(this, distributedDataSet.getName(), distributedDataSet.getClazz(), getChunkIdFromFilename(chunk.getName())));
                }

            } else {
                LOGGER.warn("DDS dir is empty: {}", distributedDataSet);
            }

            responseFuture.set(chunks);
        });


        return responseFuture;
    }


    @Override
    public <T extends Record> ListenableFuture<ChunkDataSet<T>> createChunk(DistributedDataSet<T> distributedDataSet) {
        SettableFuture<ChunkDataSet<T>> chunkDataSetSettableFuture = SettableFuture.<ChunkDataSet<T>>create();

        getExecutorService().submit(() -> {
            ChunkDataSet<T> chunkDataSet = new ChunkDataSet<>(this, distributedDataSet.getName(), distributedDataSet.getClazz(), UUID.randomUUID());

            File file = createChunkFile(chunkDataSet);

            try {
                FileOutputStream fileOutputStream = new FileOutputStream(file, true);
                BufferedOutputStream out = new BufferedOutputStream(fileOutputStream, CHUNK_INPUT_BUFFER);
                chunksOutputStreams.put(chunkDataSet.getId(), new ObjectOutputStream(out));
            } catch (IOException e) {
                e.printStackTrace();
            }

            chunkDataSetSettableFuture.set(chunkDataSet);
        });
        return chunkDataSetSettableFuture;
    }

    @Override
    public <T extends Record> ListenableFuture<ChunkDataSet<T>> createChunk(String ddsId, Class<T> clazz) {
        SettableFuture<ChunkDataSet<T>> chunkDataSetSettableFuture = SettableFuture.<ChunkDataSet<T>>create();

        getExecutorService().submit(() -> {
            ChunkDataSet<T> chunkDataSet = new ChunkDataSet<>(this, ddsId, clazz, UUID.randomUUID());

            File file = createChunkFile(chunkDataSet);

            try {
                FileOutputStream fileOutputStream = new FileOutputStream(file, true);

                chunksOutputStreams.put(chunkDataSet.getId(), new ObjectOutputStream(new BufferedOutputStream(fileOutputStream)));
            } catch (IOException e) {
                e.printStackTrace();
            }

            chunkDataSetSettableFuture.set(chunkDataSet);
        });

        return chunkDataSetSettableFuture;
    }

    @Override
    public <T extends Record> ListenableFuture<Boolean> destroyChunk(ChunkDataSet<T> chunkDataSet) {
        SettableFuture<Boolean> booleanSettableFuture = SettableFuture.<Boolean>create();

        getExecutorService().submit(() -> {
            OutputStream output = chunksOutputStreams.get(chunkDataSet.getId());

            if (output != null) {
                try {
                    output.flush();
                    output.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
                chunksOutputStreams.remove(chunkDataSet.getId());
            }

            getChunkFile(chunkDataSet).delete();

            booleanSettableFuture.set(true);
        });

        return booleanSettableFuture;
    }

    @Override
    public <T extends Record> ListenableFuture<Boolean> read(ChunkDataSet<T> chunkDataSet, Consumer<T> consumer) {
        SettableFuture<Boolean> booleanSettableFuture = SettableFuture.<Boolean>create();

        getExecutorService().submit(() -> {
            flush();
            com.codahale.metrics.Timer.Context timer = Metrics.getReadTimer().time();

            File chunkFile = getChunkFile(chunkDataSet);


            if (chunkFile.exists()) {
                try {
                    ObjectInputStream input = new ObjectInputStream(new BufferedInputStream(new FileInputStream(chunkFile)));


                    for (; ; ) {
                        try {
                            T read = (T) input.readObject();
                            consumer.accept(read);
                        } catch (EOFException e) {
                            break;
                        }

                    }


                    input.close();
                } catch (IOException | ClassNotFoundException e) {
                    e.printStackTrace();
                }
            }
            timer.stop();

            booleanSettableFuture.set(true);
        });

        return booleanSettableFuture;
    }

    @Override
    public <T extends Record> ListenableFuture<Boolean> write(ChunkDataSet<T> chunkDataSet, T object) {
        SettableFuture<Boolean> booleanSettableFuture = SettableFuture.<Boolean>create();

        getExecutorService().submit(() -> {
            com.codahale.metrics.Timer.Context timer = Metrics.getWriteTimer().time();

            ObjectOutputStream output = getChunkOutputStream(chunkDataSet);
            try {
                output.writeObject(object);
                booleanSettableFuture.set(true);
            } catch (IOException | NullPointerException e) {
                LOGGER.warn("Cant write object to chunk", e);
                booleanSettableFuture.set(false);
            }

            timer.stop();
        });

        return booleanSettableFuture;
    }

    @Override
    public <T extends Record> ListenableFuture<Long> getChunkSize(ChunkDataSet<T> chunkDataSet) {
        SettableFuture<Long> booleanSettableFuture = SettableFuture.<Long>create();

        getExecutorService().submit(() -> {
            File chunkFile = getChunkFile(chunkDataSet);
            if (!chunkFile.exists()) {
                booleanSettableFuture.set(0l);
            } else {
                booleanSettableFuture.set(chunkFile.length());
            }
        });

        return booleanSettableFuture;
    }

    @Override
    public <T extends Record> ListenableFuture<Long> getChunkElementsCount(ChunkDataSet<T> chunkDataSet) {
        return null;
    }

    @Override
    public ListenableFuture<Boolean> flush() {
        SettableFuture<Boolean> responseFuture = SettableFuture.<Boolean>create();

        getExecutorService().submit(() -> {

            AtomicBoolean hasErrors = new AtomicBoolean(false);

            chunksOutputStreams.forEach((key, item) -> {
                try {
                    item.flush();
                } catch (IOException e) {
                    LOGGER.warn("Cant flush chunk: " + key, e);

                    hasErrors.set(true);
                }
            });

            responseFuture.set(hasErrors.get());
        });

        return responseFuture;
    }

    /**
     * Get cluster information
     * <p>
     * Will retrieve information from coordinator
     *
     * @return RingInfo
     */
    @Override
    public ListenableFuture<RingInfo> getClusterInfo() {
        RingInfo ringInfo = getRing().getRingInfo();

        SettableFuture<RingInfo> ringInfoFuture = SettableFuture.<RingInfo>create();
        ringInfoFuture.set(ringInfo);

        return ringInfoFuture;
    }

    @Override
    public ListenableFuture<Boolean> elect(SortedSet<NodeInfo> activeNodes) {
        getRing().setRingStatus(RingStatus.ELECTING);

        LOGGER.debug("Electing new coordinator one of {}", activeNodes);

        NodeInfo currentCoordinator = ring.getCoordinator();

        if (activeNodes.size() <= 1) { // If singleNode mode or only one available in cluster
            NodeInfo currentNode = getNodeInfo();

            ListenableFuture<Boolean> electedFuture = elected(currentNode);

            LOGGER.warn("No available candidates for coordinator seems we are alone at this ring. Elect current node {}", currentNode.getAddress());
            return electedFuture;
        }

        if (!activeNodes.contains(getNodeInfo())) {
            activeNodes.add(getNodeInfo());

            NodeInfo nextNodeInfo = ring.getNextNode(getNodeInfo());
            Node nextNode = getNodeFactory().get(nextNodeInfo);


            ListenableFuture<Boolean> electFuture = nextNode.elect(activeNodes);

            Futures.transform(electFuture, (Boolean result) -> {
                activeNodes.remove(nextNodeInfo);

                nodeDown(nextNodeInfo);

                return elect(activeNodes);

            }, getExecutorService());

            return electFuture;
        } else {
            // We are closing Node so we decide who would be Coordinator
            NodeInfo nodeInfo = activeNodes.last();

            ListenableFuture<Boolean> electNewCoordinatorFuture = getNodeFactory().get(nodeInfo).elected(nodeInfo);

            return Futures.transform(electNewCoordinatorFuture, (Boolean result) -> {
                if (result) {
                    LOGGER.info("New coordinator was elected: {}", nodeInfo);

                    SettableFuture<Boolean> booleanSettableFuture = SettableFuture.<Boolean>create();
                    booleanSettableFuture.set(true);

                    getRing().setCoordinator(currentCoordinator, nodeInfo);

                    return booleanSettableFuture;
                }
                LOGGER.warn("New coordinator {} cant be elected. Electing new coordinator", nodeInfo.getAddress());

                activeNodes.remove(nodeInfo);

                return elect(activeNodes);
            }, getExecutorService());
        }
    }

    @Override
    public ListenableFuture<Boolean> elected(NodeInfo nodeInfo) {
        SettableFuture<Boolean> resultFuture = SettableFuture.<Boolean>create();

        getRing().updateCoordinator(nodeInfo);

        if (getNodeInfo().equals(nodeInfo)) {
            // notify nodes that we are a new coordinator

            SortedSet<NodeInfo> ringMembers = getRing().getRingMembers();

            ringMembers.forEach(item -> {
                if (!item.equals(getNodeInfo())) {
                    getNodeFactory().get(item).elected(nodeInfo);
                }
            });
        }

        ring.setRingStatus(RingStatus.RUNNING);
        resultFuture.set(true);
        return resultFuture;
    }

    @Override
    public String getNodeVersion() {
        return "1";
    }

    @Override
    public ListenableFuture<RingInfo> nodeUp(NodeInfo nodeInfo) {
        getRing().nodeUp(nodeInfo);

        SettableFuture<RingInfo> settableFuture = SettableFuture.<RingInfo>create();
        settableFuture.set(getRing().getRingInfo());


        return settableFuture;
    }

    @Override
    public ListenableFuture<Boolean> nodeDown(NodeInfo nodeInfo) {
        getRing().nodeDown(nodeInfo);

        SettableFuture<Boolean> settableFuture = SettableFuture.<Boolean>create();
        settableFuture.set(true);


        getNodeFactory().remove(nodeInfo.getAddress());

        return settableFuture;
    }

    @Override
    public ListenableFuture<Boolean> joinRing(Ring ring) {
        LOGGER.info("Joining ring: {}", ring.getName());

        this.ring = ring;

        ring.updateRingStatus(RingStatus.STARTING, RingStatus.BOOTING);

        ListenableFuture<List<RingInfo>> clusterInfoListFuture = getClusterInfoFromMembers();


        Futures.addCallback(clusterInfoListFuture, new JoinRingCallback(this, nodeFactory, executorService), getExecutorService());

        return Futures.transform(clusterInfoListFuture, (List<RingInfo> ringInfoList) -> {
            ringInfoList.forEach(ringInfo -> {
                LOGGER.info("Retrieved ring info: {}", ringInfo.getCreated());
            });

            return true;
        }, getExecutorService());
    }

    @Override
    public ListenableFuture<Boolean> updateRingInfo(RingInfo ringInfo) {
        return null;
    }

    @Override
    public ListenableFuture<Long> ping(Long time) {
        SettableFuture<Long> pingFuture = SettableFuture.<Long>create();
        pingFuture.set(new Date().getTime() - time);
        return pingFuture;
    }

    /**
     * Shutdown local node
     */
    public void shutdown() {
        LOGGER.warn("Shutdown node...");
        server.stop();
        LOGGER.warn("Node was shutdown");
    }

    protected File getPath(ChunkDataSet chunk) {
        return getChunkFile(chunk.getDataSet(), chunk.getId());
    }

    protected <T extends Record> ObjectOutputStream getChunkOutputStream(ChunkDataSet<T> chunkDataSet) {
        ObjectOutputStream objectOutputStream = chunksOutputStreams.get(chunkDataSet.getId());

        if (objectOutputStream == null) {

        }

        return objectOutputStream;
    }

    protected <T extends Record> File getChunkFile(ChunkDataSet<T> localChunk) {
        File ddsPath = getDataSetDir(localChunk.getDataSet());

        return new File(ddsPath, String.format("%s.chunk", localChunk.getId()));
    }

    protected File getChunkFile(String dds, UUID chunk) {
        File ddsPath = getDataSetDir(dds);

        return new File(ddsPath, String.format("%s.chunk", chunk));
    }

    protected UUID getChunkIdFromFilename(String name) {
        return UUID.fromString(name.substring(0, name.length() - ".chunk".length()));
    }

    protected File getDataSetDir(String dataSet) {
        return new File(getDataDir(), dataSet);
    }

    /**
     * Update information about ring
     *
     * @return RingInfo status
     */
    protected ListenableFuture<Boolean> updateRingInfo() {
        if (getRing().getCoordinator().equals(getNodeInfo())) {
            return pingAndCleanMembers();
        } else {
            return updateClusterInfo();
        }


    }

    protected Ring getRing() {
        return ring;
    }

    protected NodeFactory getNodeFactory() {
        return nodeFactory;
    }

    /**
     * Get cluster information from each members
     *
     * @return Future for list of RingInfo
     */
    private ListenableFuture<List<RingInfo>> getClusterInfoFromMembers() {
        List<ListenableFuture<RingInfo>> clusterInfoListFutures = new ArrayList<>();

        /**
         * Retrieve cluster information from all members
         */
        for (NodeInfo member : getRing().getRingMembers()) {
            if (member.equals(getNodeInfo())) {
                LOGGER.warn("Local member: {}, skip operation for retrieving information about cluster", getNodeInfo());
                continue;
            }

            Node node = getNodeFactory().get(member);
            ListenableFuture<RingInfo> clusterInfo = node.getClusterInfo();

            clusterInfoListFutures.add(clusterInfo);
        }

        return Futures.allAsList(clusterInfoListFutures);
    }

    private <T extends Record> File createChunkFile(ChunkDataSet<T> localChunk) {
        File ddsPath = getDataSetDir(localChunk.getDataSet());

        if (!ddsPath.exists()) {
            ddsPath.mkdirs();
        }

        return getChunkFile(localChunk);
    }

    private File getDataDir() {
        return DATA_DIRECTORY;
    }

    /**
     * Update information about cluster from coordinator
     *
     * @return Operation result
     */
    private ListenableFuture<Boolean> updateClusterInfo() {
        NodeInfo coordinatorInfo = getRing().getCoordinator();
        Node coordinatorNode = getNodeFactory().get(coordinatorInfo);
        ListenableFuture<RingInfo> clusterInfo = coordinatorNode.getClusterInfo();

        Futures.addCallback(clusterInfo, new FutureCallback<RingInfo>() {
            @Override
            public void onSuccess(RingInfo ringInfo) {
                getRing().updateRingInfo(ringInfo);
            }

            @Override
            public void onFailure(Throwable e) {
                LOGGER.warn("Cant retrieve ring information", e);

                if (coordinatorInfo.equals(getRing().getCoordinator())) {
                    getRing().nodeDown(coordinatorInfo);
                    getRing().updateRingStatus(RingStatus.RUNNING, RingStatus.ELECT);
                }

            }
        }, getExecutorService());

        return Futures.transform(clusterInfo, (RingInfo ringInfo) -> true, getExecutorService());
    }

    /**
     * Run over members send ping message and remove member if it's doesn't respond
     *
     * @return Operation response
     */
    private ListenableFuture<Boolean> pingAndCleanMembers() {
        List<ListenableFuture<Long>> futures = new ArrayList<>();

        List<NodeInfo> ringMembers = new ArrayList<>(getRing().getRingMembers());

        ringMembers.forEach(member -> {
            Node node = getNodeFactory().get(member);

            futures.add(node.ping(new Date().getTime()));
        });

        ListenableFuture<List<Long>> pingFutures = Futures.allAsList(futures);

        return Futures.transform(pingFutures, (List<Long> result) -> {

            for (int i = 0; i < result.size(); i++) {
                Long ping = result.get(i);

                if (ping == null) {
                    getRing().nodeDown(ringMembers.get(i));
                }
            }

            return true;
        }, getExecutorService());
    }

    private ListeningExecutorService getExecutorService() {
        return executorService;
    }

    /**
     * Create data directory if it doesn't exists
     */
    static {
        DATA_DIRECTORY.mkdirs();
    }

}
