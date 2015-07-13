package com.fnklabs.dds.coordinator;

import com.google.common.net.HostAndPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Ring worker. Walk among members then request or send actual information about ring based on current ring status
 */
class WatchDog implements Runnable {
    /**
     * logger
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(WatchDog.class);

    private LocalNode localNode;

    public WatchDog(LocalNode localNode) {
        this.localNode = localNode;
    }

    @Override
    public void run() {
        try {
            Ring ring = localNode.getRing();

            if (ring == null) {
                return;
            }
            List<HostAndPort> members = ring.getRingMembers().stream().map(NodeInfo::getAddress).collect(Collectors.toList());

            LOGGER.debug("Ring [{}] status: {} members: {}", localNode.getNodeInfo().getAddress(), ring.getRingStatus(), members);

            switch (ring.getRingStatus()) {
                case STARTING:
                    LOGGER.info("Booting node {}. Don't do anything boot in progress by another thread", ring.getCurrentNode().getAddress());

                    break;
                case BOOTING:
                    LOGGER.info("Booting node {}. Don't do anything boot in progress by another thread", ring.getCurrentNode().getAddress());

                    break;

                case RUNNING:
                    LOGGER.info("Cluster is running, all is ok");

                    localNode.updateRingInfo();

                    break;
                case ELECT:
                    LOGGER.info("Coordinator must be elected");

                    localNode.elect(localNode.getRing().getRingMembers());

                    break;
                case ELECTING:
                    LOGGER.info("Coordinator elect operation is in progress, nothing to do...");
                    break;

                case SYNCHRONIZATION:
                    LOGGER.info("Balancing was requested new nodes appeared or some nodes was down");
                    break;

                case SHUTDOWN:
                    LOGGER.info("Shutdown node requested");

                    localNode.shutdown();

                    break;
            }
        } catch (Throwable e) {
            LOGGER.warn("Cant execute", e);
        }

    }


}
