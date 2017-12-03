package com.fnklabs.dds.coordinator;

import com.fnklabs.dds.network.IncomeMessageHandler;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.concurrent.ExecutorService;

class ServerIncomeMessageHandler implements IncomeMessageHandler {
    private static final Logger LOGGER = LoggerFactory.getLogger(ServerIncomeMessageHandler.class);
    /**
     * ServerNode instance
     */
    private final ServerNode server;


    private final ExecutorService executorService;

    /**
     * @param server          ServerNode instance
     * @param executorService ExecutorService
     */
    ServerIncomeMessageHandler(ServerNode server, ExecutorService executorService) {
        this.server = server;
        this.executorService = executorService;
    }

    @Override
    public ListenableFuture<ByteBuffer> handle(ByteBuffer message) {
        return null;
//        if (message.getApiVersion() != ApiVersion.VERSION_1) {
//            SettableFuture<Message> messageSettableFuture = SettableFuture.<Message>create();
//            messageSettableFuture.set(new Message(Message.getNextId(), message.getId(), StatusCode.UNKNOWN_API_VERSION, ApiVersion.VERSION_1, new byte[0], message
//                    .getClient()));
//
//            return messageSettableFuture;
//        }
//
//        Operation operation = extractOperation(message);
//
//        if (operation instanceof NodeUp) {
//            ListenableFuture<ClusterInformation> responseFuture = server.nodeUp(((NodeUp) operation).getNodeInfo());
//
//            return Futures.transform(responseFuture, (ClusterInformation result) -> {
//                return Message.createReply(message, StatusCode.OK, ApiVersion.VERSION_1, result);
//            }, executorService);
//        } else if (operation instanceof NodeDown) {
//            ListenableFuture<Boolean> responseFuture = server.nodeDown(((NodeDown) operation).getNodeInfo());
//
//            return Futures.transform(responseFuture, (Boolean result) -> {
//                return Message.createReply(message, StatusCode.OK, ApiVersion.VERSION_1, result);
//            }, executorService);
//        } else if (operation instanceof GetNodeInfo) {
//            ListenableFuture<NodeInfo> responseFuture = server.getNodeInfo();
//
//            return Futures.transform(responseFuture, (NodeInfo result) -> {
//                return Message.createReply(message, StatusCode.OK, ApiVersion.VERSION_1, result);
//            }, executorService);
//        } else if (operation instanceof Ping) {
//            SettableFuture<Message> pingResponse = SettableFuture.<Message>create();
//            Ping ping = new Ping(((Ping) operation).getSendTime(), DateTime.now());
//            pingResponse.set(Message.createReply(message, StatusCode.OK, ApiVersion.VERSION_1, ping));
//
//            return pingResponse;
//        } else if (operation instanceof UpdateRingInfo) {
//            server.updateClusterInfo(((UpdateRingInfo) operation).getClusterInformation());
//        }
//
//        SettableFuture<Message> messageSettableFuture = SettableFuture.<Message>create();
//        messageSettableFuture.set(Message.createReply(message, StatusCode.UNKNOWN, ApiVersion.VERSION_1, new byte[0]));
//        return messageSettableFuture;
    }

//    /**
//     * Get operation object from message
//     *
//     * @param message Network message
//     *
//     * @throws CantExtractOperation if can't extract operation from Network message
//     */
//    private Operation extractOperation(Message message) {
//        try {
//            byte[] messageData = message.getMessageData();
//            ObjectInputStream objectInputStream = new ObjectInputStream(new ByteArrayInputStream(messageData));
//
//            return (Operation) objectInputStream.readObject();
//        } catch (IOException | ClassNotFoundException e) {
//
//            LOGGER.warn("Can't extract operation from message", e);
//
//            throw new CantExtractOperation();
//        }
//    }
}