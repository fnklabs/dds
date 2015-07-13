package com.fnklabs.dds.coordinator;

import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;
import java.util.SortedSet;
import java.util.function.Consumer;

public interface Node {
    /**
     * Default ring port
     */
    int DEFAULT_PORT = 10000;

    /**
     * Get node host address
     *
     * @return Host address
     */
    HostAndPort getAddress();

    /**
     * Get node information
     *
     * @return Node information
     */
    NodeInfo getNodeInfo();

    /**
     * Retrieve information about ring/cluster
     *
     * @return Future for get RingInfo operation and return null on future error
     */
    ListenableFuture<RingInfo> getClusterInfo();


    /**
     * Elect new coordinator
     *
     * @param activeNodes Current active nodes
     *
     * @return Future for  Elect operation and return null on future error
     */
    ListenableFuture<Boolean> elect(SortedSet<NodeInfo> activeNodes);

    /**
     * Notify node that it was elected like coordinator
     *
     * @param nodeInfo NodeInfo
     *
     * @return Future for Elected operation and return null on future error
     */
    ListenableFuture<Boolean> elected(NodeInfo nodeInfo);

    /**
     * Get node version
     *
     * @return Version of node
     */
    String getNodeVersion();

    /**
     * Notify current node that specified node was up and register in the ring
     *
     * @param nodeInfo New node info
     *
     * @return Future for get NodeUp operation and return null on future error
     */
    ListenableFuture<RingInfo> nodeUp(NodeInfo nodeInfo);

    /**
     * Notify current node that specified node was down and remove from the ring
     *
     * @param nodeInfo Node info
     *
     * @return Future for NodeDown operation and return null on future error
     */
    ListenableFuture<Boolean> nodeDown(NodeInfo nodeInfo);

    /**
     * Join current node to the ring and if there are active session with another ring we must unregister from it
     * <p>
     * Will retrieve coordinator from on of the member and call {@link #nodeUp(NodeInfo)}
     *
     * @param ring Ring that we must join
     *
     * @return Future for JoinRing operation and return null on future error
     */
    ListenableFuture<Boolean> joinRing(Ring ring);


    ListenableFuture<Boolean> updateRingInfo(RingInfo ringInfo);

    /**
     * Ping host and return latency time
     *
     * @param time Current time
     *
     * @return Future for Ping operation and return null on future error
     */
    ListenableFuture<Long> ping(Long time);

    /**
     * Destroy DDS and all it's chunks data on current node
     *
     * @param distributedDataSet DDS
     * @param <T>                DDS type
     */
    <T extends Record> ListenableFuture<Boolean> destroyDataSet(DistributedDataSet<T> distributedDataSet);

    /**
     * Discover and return all local chunks by specified DDS
     *
     * @param distributedDataSet DDS
     * @param <T>                DDS type
     *
     * @return Chunks list
     */
    <T extends Record> ListenableFuture<List<ChunkDataSet<T>>> discoverChunks(DistributedDataSet<T> distributedDataSet);

    /**
     * Create chunk on this node
     *
     * @param distributedDataSet DDS
     * @param <T>                DDS type
     *
     * @return New chunk
     */
    <T extends Record> ListenableFuture<ChunkDataSet<T>> createChunk(DistributedDataSet<T> distributedDataSet);

    /**
     * Create Chunk on this node
     *
     * @param ddsId DDS id
     * @param clazz DDS class type
     * @param <T>   DDS type
     *
     * @return New chunk
     */
    <T extends Record> ListenableFuture<ChunkDataSet<T>> createChunk(String ddsId, Class<T> clazz);

    /**
     * Destroy chunk on this
     *
     * @param chunkDataSet Chunk
     * @param <T>          Chunk Type
     */
    <T extends Record> ListenableFuture<Boolean> destroyChunk(ChunkDataSet<T> chunkDataSet);

    /**
     * Read data from chunk and send it to consumer row by row
     *
     * @param chunkDataSet Chunk
     * @param consumer     Row Consumer
     * @param <T>          Chunk type
     */
    <T extends Record> ListenableFuture<Boolean> read(ChunkDataSet<T> chunkDataSet, Consumer<T> consumer);

    /**
     * Write data to chunk
     *
     * @param chunkDataSet Chunk
     * @param object       data object
     * @param <T>          Chunk and data object type
     */
    <T extends Record> ListenableFuture<Boolean> write(ChunkDataSet<T> chunkDataSet, T object);

    /**
     * Retrieve chunk size
     *
     * @param chunkDataSet Chunk
     * @param <T>          Chunk type
     *
     * @return Chunk size
     */
    <T extends Record> ListenableFuture<Long> getChunkSize(ChunkDataSet<T> chunkDataSet);

    /**
     * Get chunk objects count stored in specified chunk
     *
     * @param chunkDataSet Chunk
     * @param <T>          Chunk type
     *
     * @return Object count
     */
    <T extends Record> ListenableFuture<Long> getChunkElementsCount(ChunkDataSet<T> chunkDataSet);

    /**
     * Flush all data from stream to storage
     */
    ListenableFuture<Boolean> flush();
}
