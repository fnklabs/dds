package com.fnklabs.dds.coordinator;

import com.fnklabs.dds.coordinator.operation.*;
import com.fnklabs.dds.network.NetworkClient;
import com.fnklabs.dds.network.ClientException;
import com.google.common.base.Objects;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.ListenableFuture;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Client to server node
 */
class ServerNodeClient implements Node {

    private static final Logger LOGGER = LoggerFactory.getLogger(ServerNodeClient.class);

    /**
     * Client to remote node
     */
    @NotNull
    private final AtomicReference<NetworkClient> networkClient = new AtomicReference<>();

    /**
     *
     */
    @NotNull
    private final ExecutorService executorService;

    /**
     * Remote node address
     */
    @NotNull
    private final HostAndPort remoteAddress;

    /**
     * @param remoteAddress   Remote node address
     * @param executorService Executor service
     *
     * @throws ClientException if can't connect to remote node
     */
    protected ServerNodeClient(@NotNull HostAndPort remoteAddress, @NotNull ExecutorService executorService) {
        this.remoteAddress = remoteAddress;
        this.executorService = executorService;

//        try {
//            NetworkClient build = NetworkClientFactory.build(remoteAddress, new Consumer<Message>() {
//                @Override
//                public void accept(Message message) {
//                    LoggerFactory.getLogger(ServerNodeClient.class).debug("New system message: {}", message.getId());
//                }
//            });
//            networkClient.set(build);
//        } catch (ClientException e) {
//            LOGGER.warn("Can't connect to remote node", e);
//        }
    }

    /**
     * Get remote address
     *
     * @return Remote node address
     */
    @Override
    public HostAndPort getAddressAndPort() {
        return remoteAddress;
    }

    /**
     * Get remote node information.
     * <p>
     * Will retrieve actual information about node from remote node
     *
     * @return Future for Remote node info
     */
    @Override
    public ListenableFuture<NodeInfo> getNodeInfo() {
        return send(new GetNodeInfo());
    }

    /**
     * Get cluster information from remote node
     *
     * @return Ring information
     */
    @Override
    public ListenableFuture<ClusterInformation> getClusterInfo() {
        return send(new GetClusterInfo());
    }

    /**
     * Send notification to remote node that specified node was up
     *
     * @param nodeInfo New node that was up
     *
     * @return Response Future. True if operation was accepted by remote node False otherwise
     */
    @Override
    public ListenableFuture<ClusterInformation> nodeUp(NodeInfo nodeInfo) {
        return send(new NodeUp(nodeInfo));
    }

    /**
     * Send notification to remote node that specified not was down
     *
     * @param nodeInfo Node that was down
     *
     * @return Response future. True if operation was accepted by remote node False otherwise
     */
    @Override
    public ListenableFuture<Boolean> nodeDown(NodeInfo nodeInfo) {
        return send(new NodeDown(nodeInfo));
    }

    @Override
    public ListenableFuture<Boolean> repair(ClusterInformation clusterInformation) {
        return null;
    }

    /**
     * Send notification to remote node that cluster information was updated
     *
     * @param clusterInformation New cluster information
     *
     * @return Response Future. True if operation was accepted by remote node False otherwise
     */
    @Override
    public ListenableFuture<Boolean> updateClusterInfo(ClusterInformation clusterInformation) {
        return send(new UpdateRingInfo(clusterInformation));
    }

    /**
     * Send ping to remote node and return latency (diff of time when ping messages was send to remote node and when reply was retrieved)
     *
     * @return Future for Ping operation and return null on future error
     */
    @Override
    public ListenableFuture<Long> ping() {
//        return Futures.transform(send(new Ping()), (Ping input) -> {
//            return input.getReceivedTime().getMillis() - input.getSendTime().getMillis();
//        }, executorService);

        return null;
    }

    /**
     * Close connection to remote node
     */
    @Override
    public void close() {
        NetworkClient networkClient = this.networkClient.get();

        if (networkClient != null) {
            networkClient.close();
        }
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(remoteAddress);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof ServerNodeClient) {
            return Objects.equal(((ServerNodeClient) obj).remoteAddress, this.remoteAddress);
        }

        return false;
    }

    /**
     * Send operation to remote node than deserialize object and cast it to {@code O} class type type
     *
     * @param data Operation that must be send to remote node
     * @param <O>  Type of class to which response objected will be casted
     * @param <I>  Input operation type
     *
     * @return Future for response
     */
    private <O, I extends Operation> ListenableFuture<O> send(@NotNull I data) {
//        return send(data, new Function<Message, O>() {
//            @Override
//            public O apply(Message message) {
//                if (message.getStatusCode() != StatusCode.OK) {
//                    throw new RequestException(message.getStatusCode());
//                }
//
//                try {
//                    ObjectInputStream inputStream = new ObjectInputStream(new ByteArrayInputStream(message.getMessageData()));
//                    return (O) inputStream.readObject();
//                } catch (IOException | ClassNotFoundException e) {
//                    LOGGER.warn("Can't unserialize response message");
//
//                    throw new RequestException(e);
//                }
//
//            }
//        });

        return null;
    }
//
//    private <O, I extends Operation> ListenableFuture<O> send(@NotNull I data, @NotNull Function<Message, O> transformationFuture) {
//        try {
//            ByteArrayOutputStream out = new ByteArrayOutputStream();
//            ObjectOutputStream objectOutputStream = new ObjectOutputStream(out);
//
//            objectOutputStream.writeObject(data);
//
//            return send(ByteBuffer.wrap(out.toByteArray()), transformationFuture);
//        } catch (IOException e) {
//            LOGGER.warn("Can't send message", e);
//
//            throw new RequestException(e);
//        }
//    }
//
//    private <O> ListenableFuture<O> send(@NotNull ByteBuffer data, @NotNull Function<Message, O> transformationFuture) {
//
//        try {
//            ResponseFuture send = getNetworkClient().send(data);
//            return Futures.transform(send, (Message input) -> {
//                return transformationFuture.apply(input);
//            }, executorService);
//
//        } catch (ClientException e) {
//            LOGGER.warn("Client problem", e);
//
//            SettableFuture<O> exceptionFuture = SettableFuture.<O>create();
//            exceptionFuture.setException(e);
//
//            return exceptionFuture;
//        }
//    }
//
//    @NotNull
//    private synchronized NetworkClient getNetworkClient() throws ClientException {
//        NetworkClient networkClient = this.networkClient.get();
//
//        if (networkClient == null) {
//            NetworkClient build = NetworkClientFactory.build(remoteAddress, new Consumer<Message>() {
//                @Override
//                public void accept(Message message) {
//                    LoggerFactory.getLogger(ServerNodeClient.class).debug("New system message: {}", message.getId());
//                }
//            });
//
//            this.networkClient.set(build);
//
//        }
//        return this.networkClient.get();
//    }

}
