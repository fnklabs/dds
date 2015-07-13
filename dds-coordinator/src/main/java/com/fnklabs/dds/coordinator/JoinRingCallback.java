package com.fnklabs.dds.coordinator;


import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;

class JoinRingCallback implements FutureCallback<List<RingInfo>> {
    public static final Logger LOGGER = LoggerFactory.getLogger(JoinRingCallback.class);

    private LocalNode localNode;
    private NodeFactory nodeFactory;
    private ExecutorService executorService;

    public JoinRingCallback(LocalNode localNode, NodeFactory nodeFactory, ExecutorService executorService) {
        this.localNode = localNode;
        this.nodeFactory = nodeFactory;
        this.executorService = executorService;
    }

    @Override
    public void onSuccess(List<RingInfo> result) {
        if (result.isEmpty()) {
            startSingleNodeMode();
        } else {
            Optional<RingInfo> ringInfoOptional = getLatestRingInfo(result);

            if (ringInfoOptional.isPresent()) {
                startMultipleNodesMode(ringInfoOptional.get());
            } else {
                LOGGER.warn("Ring information was not retrieved");

                updateRingStatus(RingStatus.BOOTING, RingStatus.STARTING);
            }
        }
    }

    @Override
    public void onFailure(@NotNull Throwable t) {
        LOGGER.warn("Cant retrieve cluster information", t);
    }

    private void startMultipleNodesMode(RingInfo ringInfo) {
        getRing().updateRingInfo(ringInfo);

        NodeInfo currentNodeInfo = getRing().getCurrentNode();
        NodeInfo coordinatorNodeInfo = getRing().getCoordinator();

        Node coordinatorNode = getNodeFactory().get(coordinatorNodeInfo);
        ListenableFuture<RingInfo> nodeUpFuture = coordinatorNode.nodeUp(currentNodeInfo);

        Futures.addCallback(nodeUpFuture, new FutureCallback<RingInfo>() {
            @Override
            public void onSuccess(RingInfo result) {
                getRing().getRingMembers().forEach(nodeInfo -> localNode.nodeUp(nodeInfo));

                updateRingStatus(RingStatus.BOOTING, RingStatus.RUNNING);
            }

            @Override
            public void onFailure(@NotNull Throwable t) {
                LOGGER.warn("Cant node up", t);
                updateRingStatus(RingStatus.BOOTING, RingStatus.STARTING);
            }
        }, executorService);
    }

    private void updateRingStatus(RingStatus expected, RingStatus newStatus) {
        RingInfo prevRingInfo = ConfigurationStore.read();

        if (newStatus == RingStatus.RUNNING && prevRingInfo != null) {
            getRing().updateRingStatus(expected, RingStatus.SYNCHRONIZATION);
        } else {
            getRing().updateRingStatus(expected, newStatus);
        }
    }

    private void startSingleNodeMode() {
        LOGGER.warn("Seems I'm alone node the ring :`( So I will be coordinator");

        getRing().setCoordinator(null, getNodeInfo());

        updateRingStatus(RingStatus.BOOTING, RingStatus.RUNNING);
    }

    private Optional<RingInfo> getLatestRingInfo(List<RingInfo> result) {
        return result
                .parallelStream()
                .filter(item -> item != null)
                .sorted(
                        (left, right) -> Long.compare(right.getCreated(), left.getCreated())
                )
                .findFirst();
    }

    private NodeFactory getNodeFactory() {
        return nodeFactory;
    }

    private NodeInfo getNodeInfo() {
        return localNode.getNodeInfo();
    }

    private Ring getRing() {
        return localNode.getRing();
    }
}
