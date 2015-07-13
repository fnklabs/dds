package com.fnklabs.dds.coordinator;

import com.fnklabs.dds.Metrics;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

public class DistributedDataSet<T extends Record> {
    public static final int DEFAULT_REPLICATION_FACTOR = 2;

    private short replicationFactor = DEFAULT_REPLICATION_FACTOR;

    private String name;

    /**
     * Chunk DDS locator
     */
    private NodeFactory nodeFactory;

    /**
     * DDS class type
     */
    private Class<T> clazz;

    /**
     * All DDS chunks
     */
    private List<ChunkDataSet<T>> chunks = Collections.synchronizedList(new ArrayList<>());

    /**
     * Reference on current chunk
     */
    private AtomicReference<ChunkDataSet<T>> currentChunk = new AtomicReference<>();

    private LoadBalancingPolicy loadBalancingPolicy;


    /**
     * Use to load existing dataSet
     *
     * @param name                DDS ID
     * @param nodeFactory         Chunk Locator
     * @param clazz               DDS class type
     * @param loadBalancingPolicy LoadBalancingPolicy
     */
    protected DistributedDataSet(String name, NodeFactory nodeFactory, Class<T> clazz, LoadBalancingPolicy loadBalancingPolicy) {
        this.name = name;
        this.nodeFactory = nodeFactory;
        this.clazz = clazz;
        this.loadBalancingPolicy = loadBalancingPolicy;
    }

    /**
     * Create new dataSet
     *
     * @param clazz
     * @param chunks
     * @param nodeFactory
     * @param loadBalancingPolicy
     */
    protected DistributedDataSet(Class<T> clazz, List<ChunkDataSet<T>> chunks, NodeFactory nodeFactory, LoadBalancingPolicy loadBalancingPolicy) {
        this.clazz = clazz;
        this.chunks = chunks;
        this.nodeFactory = nodeFactory;
        this.loadBalancingPolicy = loadBalancingPolicy;
    }

    protected DistributedDataSet(NodeFactory nodeFactory, Class<T> clazz, LoadBalancingPolicy loadBalancingPolicy) {
        this.nodeFactory = nodeFactory;
        this.clazz = clazz;
        this.loadBalancingPolicy = loadBalancingPolicy;
    }

    protected DistributedDataSet(NodeFactory nodeFactory, List<T> items, Class<T> clazz, LoadBalancingPolicy loadBalancingPolicy) {
        this.nodeFactory = nodeFactory;
        this.clazz = clazz;
        this.loadBalancingPolicy = loadBalancingPolicy;

        write(items);
    }

    protected DistributedDataSet(String name, NodeFactory nodeFactory, Class<T> clazz, List<ChunkDataSet<T>> chunks, LoadBalancingPolicy loadBalancingPolicy) {
        this.name = name;
        this.nodeFactory = nodeFactory;
        this.clazz = clazz;
        this.chunks = chunks;
        this.loadBalancingPolicy = loadBalancingPolicy;
    }

    public String getName() {
        return name;
    }

    public List<ChunkDataSet<T>> getChunks() {
        return Collections.unmodifiableList(chunks);
    }

    public Class<T> getClazz() {
        return clazz;
    }

    /**
     * Map
     *
     * @param mapFunction
     * @param newChunkClass
     * @param <O>
     *
     * @return
     */
    public <O extends Record> DistributedDataSet<O> map(Function<T, O> mapFunction, Class<O> newChunkClass) {
        com.codahale.metrics.Timer.Context timer = Metrics.getTimer(Metrics.Type.DDS_MAP_OPERATION).time();
        List<ChunkDataSet<O>> newChunks = Collections.synchronizedList(new ArrayList<>());

        DistributedDataSet<O> newDds = new DistributedDataSet<O>(newChunkClass, newChunks, nodeFactory, loadBalancingPolicy);

        chunks.parallelStream().forEach(chunk -> {
            List<ChunkDataSet<O>> map = chunk.map(newChunkClass, newDds.getName(), mapFunction);

            newChunks.addAll(map);
        });

        timer.stop();

        return newDds;
    }

    public void foreach(Consumer<T> consumer) {
        com.codahale.metrics.Timer.Context time = Metrics.getTimer(Metrics.Type.DDS_FOREACH_OPERATION).time();
        chunks.parallelStream().forEach(chunk -> {
            chunk.foreach(consumer);
        });
        time.stop();
    }


    public T reduce(BiFunction<T, T, T> reducer) {
        com.codahale.metrics.Timer.Context time = Metrics.getTimer(Metrics.Type.DDS_REDUCE_OPERATION).time();

        List<T> result = Collections.synchronizedList(new ArrayList<T>());

        chunks.parallelStream().forEach(chunk -> result.add(chunk.reduce(reducer)));

        Optional<T> reduceResult = result.parallelStream().reduce(reducer::apply);

        time.stop();

        return reduceResult.get();
    }

    public <O extends Record> List<DistributedDataSet<O>> transform(Function<T, O> transformFunction) {
        return null;
    }

    public <O extends Record, K> Map<K, DistributedDataSet<O>> group(Function<T, O> groupFunction) {
        return null;
    }

    public void write(List<T> items) {
        items.forEach(this::write);
    }

    public ListenableFuture<Boolean> write(T item) {

        return Futures.transform(getCurrentChunk(), (ChunkDataSet<T> chunk) -> {
            chunk.write(item);
            return true;
        }, MoreExecutors.sameThreadExecutor());
    }


    /**
     * Add new chunk to dataSet
     *
     * @param chunkDataSet Chunk
     */
    public void add(ChunkDataSet<T> chunkDataSet) {
        chunks.add(chunkDataSet);
    }

    protected ListenableFuture<ChunkDataSet<T>> createNewChunk() {
        NodeInfo next = loadBalancingPolicy.next();

        Node node = nodeFactory.get(next);
        return node.createChunk(this);
    }

    protected ListenableFuture<ChunkDataSet<T>> getCurrentChunk() {
        ChunkDataSet<T> chunkDataSet = currentChunk.get();

        if (chunkDataSet == null) {
            return Futures.transform(createNewChunk(), (ChunkDataSet<T> newChunk) -> {
                currentChunk.set(newChunk);

                return newChunk;
            }, MoreExecutors.sameThreadExecutor());

        } else {
            SettableFuture<ChunkDataSet<T>> responseFuture = SettableFuture.<ChunkDataSet<T>>create();
            responseFuture.set(chunkDataSet);
            return responseFuture;
        }


    }
}
