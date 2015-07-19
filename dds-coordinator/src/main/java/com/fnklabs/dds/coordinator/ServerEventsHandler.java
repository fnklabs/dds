package com.fnklabs.dds.coordinator;

import com.fnklabs.dds.Message;
import com.fnklabs.dds.network.exception.RequestException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ring Network request handler
 */
class ServerEventsHandler {

    /**
     * Server events handler
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(ServerEventsHandler.class);

    /**
     * Local node
     */
    private LocalNode localNode;

    public ServerEventsHandler(LocalNode localNode) {
        this.localNode = localNode;
    }

//    @Override
//    public ConnectorMessageBuffer handle(RequestBuffer requestBuffer) throws RequestException {

//        return null;
//        try {
//            return processRequest(requestBuffer);
//        } catch (Exception e) {
//            LOGGER.warn("Cant execute request", e);
//        }
//
//        int operationCode = requestBuffer.getOperationCode();
//        OperationType operationType = OperationType.valueOf(operationCode);
//        return ResponseHelper.pack(requestBuffer.getId(), operationType, StatusCode.UNKNOWN);
//    }

//    @Override
    public Message handle(Message requestBuffer) throws RequestException {
        return null;
    }

//    protected ByteBuffer processRequest(RequestBuffer requestBuffer) {
//        ByteBuffer response;

//        int operationCode = requestBuffer.getOperationCode();
//
//        OperationType operationType = OperationType.valueOf(operationCode);
//
//        switch (operationType) {
//            case CLUSTER_INFO: {
//                LOGGER.info("New node requesting cluster info: {}");
//
//                RingInfo ringInfo = getRing().getRingInfo();
//
//                response = ConnectorMessageBufferUtils.pack(requestBuffer.getId(), operationType, ringInfo);
//                break;
//            }
//            case PING:
//                LOGGER.info("New node requesting ping: {}");
//
//                DateTime time = RequestHelper.<DateTime>unpack(requestBuffer.getData());
//
//                DateTime now = DateTime.now(time.getChronology());
//
//                Interval interval = new Interval(time, now);
//
//                response = ConnectorMessageBufferUtils.pack(requestBuffer.getId(), operationType, interval.toDurationMillis());
//                break;
//            case NODE_UP: {
//                NodeInfo nodeInfo = RequestHelper.<NodeInfo>unpack(requestBuffer.getData());
////                NodeInfo nodeInfo = request.<NodeInfo>unpack();
//
//                LOGGER.info("Node up: {}", nodeInfo.getAddress());
//
//                getLocalNode().nodeUp(nodeInfo);
//
//                response = ConnectorMessageBufferUtils.pack(requestBuffer.getId(), operationType, getRing().getRingInfo());
//            }
//            break;
//            case NODE_DOWN: {
//                NodeInfo nodeInfo = RequestHelper.<NodeInfo>unpack(requestBuffer.getData());// request.<NodeInfo>unpack();
//
//                getLocalNode().nodeDown(nodeInfo);
//
//                LOGGER.info("Node down: {}", nodeInfo.getAddress());
//
//                response = ConnectorMessageBufferUtils.pack(requestBuffer.getId(), operationType, StatusCode.OK);
//            }
//            break;
//            case ELECT_COORDINATOR:
//
//                Elect elect = RequestHelper.<Elect>unpack(requestBuffer.getData());// request.<Elect>unpack();
//                LOGGER.info("New coordinator must be elected: {} {}", elect.getElected(), elect.getCreated());
//
//                getLocalNode().elect(elect.getElected());
//
//                response = ConnectorMessageBufferUtils.pack(requestBuffer.getId(), operationType, StatusCode.OK);
//
//                break;
//            case ELECTED_COORDINATOR:
//                NodeInfo nodeInfo = RequestHelper.unpack(requestBuffer.getData());//request.<NodeInfo>unpack();
//
//                LOGGER.info("New coordinator was elected: {}", nodeInfo.getAddress());
//
//                getLocalNode().elected(nodeInfo);
//
//                response = ConnectorMessageBufferUtils.pack(requestBuffer.getId(), operationType, StatusCode.OK);
//
//                break;
//            default:
//                response = ConnectorMessageBufferUtils.pack(requestBuffer.getId(), operationType, StatusCode.UNKNOWN_OPERATION_TYPE);
//                break;
//        }
//        return response;
//    }

    protected Ring getRing() {
        return getLocalNode().getRing();
    }

    private LocalNode getLocalNode() {
        return localNode;
    }

}
