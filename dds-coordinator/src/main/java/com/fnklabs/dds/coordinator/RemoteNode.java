package com.fnklabs.dds.coordinator;

import com.fnklabs.dds.coordinator.operation.Elect;
import com.fnklabs.dds.OperationType;
import com.fnklabs.dds.network.client.Client;
import com.fnklabs.dds.network.Message;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.ListeningExecutorService;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;
import java.util.SortedSet;
import java.util.function.Consumer;

public class RemoteNode implements Node {

    private HostAndPort address;
    private Client client;
    private ListeningExecutorService executorService;


    public RemoteNode(HostAndPort address, ListeningExecutorService executorService) throws IOException {
        this.address = address;
        this.executorService = executorService;

//        client = Client.create(address.getHostText(), new Consumer<ConnectorMessageBuffer>() {
//            @Override
//            public void accept(ConnectorMessageBuffer connectorMessageBuffer) {
//                LoggerFactory.getLogger(RemoteNode.class).debug("Retrieved new event from server: {}:{}", connectorMessageBuffer.getId(), connectorMessageBuffer.getOperationCode());
//            }
//        });
    }

    public void shutdown() {
        getClient().close();
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
    public String getNodeVersion() {
        return "1";
    }

    @Override
    public ListenableFuture<RingInfo> getClusterInfo() {
        return transform(send(OperationType.CLUSTER_INFO, getNodeInfo()));
    }

    @Override
    public ListenableFuture<RingInfo> nodeUp(NodeInfo nodeInfo) {
        return transform(send(OperationType.NODE_UP, nodeInfo));
    }

    @Override
    public ListenableFuture<Boolean> elect(SortedSet<NodeInfo> activeNodes) {
        return transformStatusIsOk(send(OperationType.ELECT_COORDINATOR, new Elect(getNodeInfo(), activeNodes)));
    }

    @Override
    public ListenableFuture<Boolean> elected(NodeInfo nodeInfo) {
        return transformStatusIsOk(send(OperationType.ELECTED_COORDINATOR, nodeInfo));
    }

    @Override
    public ListenableFuture<Boolean> nodeDown(NodeInfo nodeInfo) {
        return transformStatusIsOk(send(OperationType.NODE_DOWN, nodeInfo));
    }

    @Override
    public ListenableFuture<Boolean> joinRing(Ring ring) {
        return null;
    }

    @Override
    public ListenableFuture<Boolean> updateRingInfo(RingInfo ringInfo) {
        return null;
    }

    @Override
    public ListenableFuture<Long> ping(Long time) {
        return null;
//        return transform(client.send(OperationType.PING, time));
    }

    @Override
    public <T extends Record> ListenableFuture<Boolean> destroyDataSet(DistributedDataSet<T> distributedDataSet) {
        return null;
    }

    @Override
    public <T extends Record> ListenableFuture<List<ChunkDataSet<T>>> discoverChunks(DistributedDataSet<T> distributedDataSet) {
        return null;
    }

    @Override
    public <T extends Record> ListenableFuture<ChunkDataSet<T>> createChunk(DistributedDataSet<T> distributedDataSet) {
        return null;
    }

    @Override
    public <T extends Record> ListenableFuture<ChunkDataSet<T>> createChunk(String ddsId, Class<T> clazz) {
        return null;
    }

    @Override
    public <T extends Record> ListenableFuture<Boolean> destroyChunk(ChunkDataSet<T> chunkDataSet) {
        return null;
    }

    @Override
    public <T extends Record> ListenableFuture<Boolean> read(ChunkDataSet<T> chunkDataSet, Consumer<T> consumer) {
        return null;
    }

    @Override
    public <T extends Record> ListenableFuture<Boolean> write(ChunkDataSet<T> chunkDataSet, T object) {
        return null;
    }

    @Override
    public <T extends Record> ListenableFuture<Long> getChunkSize(ChunkDataSet<T> chunkDataSet) {
        return null;
    }

    @Override
    public <T extends Record> ListenableFuture<Long> getChunkElementsCount(ChunkDataSet<T> chunkDataSet) {
        return null;
    }


    @Override
    public ListenableFuture<Boolean> flush() {
        return null;
    }

    private Client getClient() {
        return client;
    }

    /**
     * Transform response future to required object class
     *
     * @param response Response future
     *
     * @return Future for response transformation
     */
    private ListenableFuture<Boolean> transformStatusIsOk(ListenableFuture<Message> response) {
        return Futures.transform(response, (Message message) -> {
            return message.isOk();
        }, executorService);
    }

    /**
     * Transform response future to required object class
     *
     * @param response Response future
     * @param <T>      Required object class type
     *
     * @return Future for response transformation
     */
    private <T> ListenableFuture<T> transform(ListenableFuture<Message> response) {
        return Futures.transform(response, (Message item) -> {
            if (!item.isOk()) {
                return null;
            }

            return null;
//            return ConnectorMessageBufferUtils.<T>unpack(item.getData());
        }, executorService);
    }

    /**
     * Send request to remote node
     *
     * @param operationType Request operation type
     * @param object        Request object
     *
     * @return Response future
     */
    private ListenableFuture<Message> send(OperationType operationType, Serializable object) {
        return null;//getClient().send(operationType, object);
    }
}
