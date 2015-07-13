package com.fnklabs.dds.coordinator;

import com.codahale.metrics.Timer;
import com.fnklabs.dds.Metrics;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

public class ChunkDataSet<T extends Record> {
    private AtomicInteger operationsCount = new AtomicInteger(0);
    private Node owner;
    private String dataSet;
    private Class<T> clazz;
    private UUID id;

    public ChunkDataSet(Node owner, String dataSet, Class<T> clazz, UUID id) {
        this.owner = owner;
        this.dataSet = dataSet;
        this.clazz = clazz;
        this.id = id;
    }

    public String getDataSet() {
        return dataSet;
    }

    public UUID getId() {
        return id;
    }

    public void write(T object) {
        owner.write(this, object);
    }

    /**
     * Execute function over chunk
     *
     * @param <O>            New Chunk type
     * @param clazz          New Chunk type
     * @param destinationDDS Destination DSS to which will belong new Chunks
     * @param function       Map function
     *
     * @return New chunk data set
     */
    public <O extends Record> List<ChunkDataSet<O>> map(Class<O> clazz, String destinationDDS, Function<T, O> function) {

        Timer.Context timer = Metrics.getTimer(Metrics.Type.CHUNK_MAP_OPERATION).time();

        List<ChunkDataSet<O>> newChunks = new ArrayList<>();

        AtomicReference<ChunkDataSet<O>> newChunkRef = new AtomicReference<>();
//        newChunkRef.set(getOwner().createChunk(destinationDDS, clazz));
        newChunks.add(newChunkRef.get());


        owner.read(this, new Consumer<T>() {
            @Override
            public void accept(T t) {

                newChunkRef.get().write(function.apply(t));

            }
        });


        timer.stop();

        return newChunks;
    }


    public T reduce(BiFunction<T, T, T> reducer) {
        Timer.Context time = Metrics.getTimer(Metrics.Type.CHUNK_REDUCE_OPERATION).time();

        AtomicReference<T> prevValueRef = new AtomicReference<>();

        owner.read(this, new Consumer<T>() {
            @Override
            public void accept(T nextValue) {
                T prevValue = prevValueRef.get();

                if (prevValue == null) {
                    prevValueRef.set(nextValue);
                } else {
                    T apply = reducer.apply(prevValue, nextValue);

                    prevValueRef.set(apply);
                }
            }
        });

        time.stop();

        return prevValueRef.get();
    }

    /**
     * Apply consumer to chunk. Good for Logs
     *
     * @param consumer Consumer
     */
    public void foreach(Consumer<T> consumer) {
        Timer.Context time = Metrics.getTimer(Metrics.Type.CHUNK_FOREACH_OPERATION).time();

        owner.read(this, consumer);

        time.stop();
    }

    public File getPath() {
//        return ((LocalDataNode) getOwner()).getPath(this);
        return null;
    }


    public long getChunkSize() {
        return getPath().getFreeSpace();
    }

    public boolean remove() {
        return getPath().delete();
    }


}
