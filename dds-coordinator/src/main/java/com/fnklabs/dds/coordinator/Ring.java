package com.fnklabs.dds.coordinator;

import com.fnklabs.dds.coordinator.exception.RingException;
import com.google.common.net.HostAndPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class Ring {


    private static final short REPLICATION_FACTOR = 2;

    /**
     * Ring logger
     */
    private static final Logger LOGGER = LoggerFactory.getLogger(Ring.class);
    /**
     * Load balancer
     */
    private final LoadBalancingPolicy loadBalancingPolicy = new RoundRobinLoadBalancingPolicy();


    /**
     * Current active ring
     */
    private SortedSet<NodeInfo> ringMembers = Collections.synchronizedSortedSet(new TreeSet<>());
    /**
     * Local node information
     */
    private NodeInfo localNode;
    /**
     * Current coordinator
     */
    private AtomicReference<NodeInfo> coordinator = new AtomicReference<>();
    /**
     * Ring status
     */
    private AtomicReference<RingStatus> ringStatus = new AtomicReference<>(RingStatus.STARTING);
    /**
     * Last time when ring was updated
     */
    private AtomicLong lastUpdatedRingInformation = new AtomicLong(0);
    /**
     * Ring name
     */
    private String name;

    /**
     * Create ring information
     *
     * @param members     Cluster members
     * @param localNode   Local node information
     * @param nodeFactory Node Factory/Registry
     * @param name        Ring name
     *
     * @throws RingException
     */
    public Ring(Set<HostAndPort> members, HostAndPort localNode, NodeFactory nodeFactory, String name) throws RingException {
        this.name = name;
        this.localNode = nodeFactory.get(localNode).getNodeInfo();

        members.parallelStream().forEach(address -> {
            Node node = nodeFactory.get(address);
            ringMembers.add(node.getNodeInfo());
        });

        dumpMembers();
    }


    /**
     * Retrieve key owners by internal key
     *
     * @param key Internal key
     *
     * @return Key owners
     */
    public Set<NodeInfo> getKeyOwners(byte[] key) {
        HashSet<NodeInfo> owners = new HashSet<>();

        getMembers().forEach(nodeInfo -> {
            nodeInfo.getBuckets().forEach(bucket -> {
                if (bucket.contains(Partitioner.buildToken(key))) {
                    owners.add(nodeInfo);
                }
            });
        });

        return Collections.unmodifiableSet(owners);
    }

    /**
     * Retrieve ring status
     *
     * @return RingStatus
     */
    public RingStatus getRingStatus() {
        return ringStatus.get();
    }

    /**
     * Update ring status
     *
     * @param status Ring status
     */
    protected void setRingStatus(RingStatus status) {
        ringStatus.set(status);
    }

    public String getName() {
        return name;
    }

    protected void updateCoordinator(NodeInfo member) {
        setCoordinator(getCoordinator(), member);
    }

    /**
     * Dump members to log
     */
    protected void dumpMembers() {
        ringMembers.forEach(member -> LOGGER.info("Cluster member: {}", member.getAddress()));
    }

    /**
     * Update current ring status
     *
     * @param expected  Expected status
     * @param newStatus New Status
     *
     * @return true if successfully updated false otherwise
     */
    protected boolean updateRingStatus(RingStatus expected, RingStatus newStatus) {
        boolean result = ringStatus.compareAndSet(expected, newStatus);
        if (!result) {
            LOGGER.warn("Cant update ring status");
        }

        return result;
    }

    protected NodeInfo getCoordinator() {
        return coordinator.get();
    }


    /**
     * Update ring information by specified RingInfo
     * <p>
     * If ring info created time less then current last updated ring information than will skip update opration
     *
     * @param ringInfo RingInfo
     */
    protected void updateRingInfo(RingInfo ringInfo) {
        if (lastUpdatedRingInformation.get() < ringInfo.getCreated()) {
            lastUpdatedRingInformation.set(ringInfo.getCreated());

            SortedSet<NodeInfo> members = ringInfo.getMembers();

            List<HostAndPort> membersAddress = members.stream().map(NodeInfo::getAddress).collect(Collectors.toList());

            LOGGER.info("Ring information on {}. Coordinator {} members: {}", ringInfo.getCreated(), ringInfo.getCoordinator(), membersAddress);

            NodeInfo coordinator = ringInfo.getCoordinator();

            this.coordinator.set(coordinator);

            // clean current members that cant be down
            getRingMembers().forEach(member -> {
                if (!members.contains(member)) {
                    nodeDown(member);
                }
            });

            // update members set from ring
            members.forEach(member -> {
                if (!getRingMembers().contains(member)) {
                    nodeUp(member);
                }
            });
        } else {
            LOGGER.warn("Retrieved ring information is old, skip update operation. Local {} Remote {}", lastUpdatedRingInformation.get(), ringInfo.getCreated());
        }
    }

    /**
     * Update information about new coordinator in the ring
     *
     * @param expectedCoordinator expected ring coordinator
     * @param newCoordinator      new coordinator
     */
    protected void setCoordinator(NodeInfo expectedCoordinator, NodeInfo newCoordinator) {
        if (!coordinator.compareAndSet(expectedCoordinator, newCoordinator)) {
            LOGGER.warn(
                    "Seems coordinator was set by another thread. Current: {} Expected: {} New: {}",
                    coordinator.get().getAddress(),
                    expectedCoordinator == null ? null : expectedCoordinator.getAddress(),
                    newCoordinator.getAddress());
        } else {
            LOGGER.info("Coordinator was changed to: {}", getCoordinator().getAddress());
        }
    }

    /**
     * Get ring information
     *
     * @return RingInfo
     */
    protected RingInfo getRingInfo() {
        return new RingInfo(getCoordinator(), sort(getRingMembers()), System.currentTimeMillis(), localNode);
    }

    /**
     * Register new node in the ring
     *
     * @param node New node
     */
    protected void nodeUp(NodeInfo node) {
        LOGGER.warn("Node down: {}", node);

        getRingMembers().add(node);
        loadBalancingPolicy.add(node);
    }

    /**
     * Remove node from the ring
     *
     * @param node Ring node
     */
    protected void nodeDown(NodeInfo node) {
        LOGGER.warn("Node down: {}", node);
        getRingMembers().remove(node);

        loadBalancingPolicy.remove(node);
    }

    protected SortedSet<NodeInfo> getRingMembers() {
        return ringMembers;
    }

    /**
     * Get current node from the members
     *
     * @return Current node
     */
    protected NodeInfo getCurrentNode() {
        return localNode;
    }

    /**
     * Get next (right) node from specified node position
     *
     * @param node Node position
     *
     * @return Next node
     */
    protected NodeInfo getNextNode(NodeInfo node) {
        List<NodeInfo> members = getMembers();
        int index = members.indexOf(node);

        int nextMemberIndex = (index + 1) % members.size();

        return members.get(nextMemberIndex);
    }

    /**
     * Get next (right) node from the ring
     *
     * @return Next node
     */
    protected NodeInfo getRightNode() {
        NodeInfo node = getCurrentNode();

        return getNextNode(node);
    }

    /**
     * Get ring members including current node
     *
     * @return Ring members
     */
    private List<NodeInfo> getMembers() {
        return ringMembers.stream().collect(Collectors.toList());
    }


    /**
     * Sort members asc by address
     *
     * @param members Members list
     *
     * @return Sorting address by descending
     */
    protected static SortedSet<NodeInfo> sort(Set<NodeInfo> members) {
        TreeSet<NodeInfo> nodeInfoList = new TreeSet<>();

        nodeInfoList.addAll(members);

        return nodeInfoList;
    }

}
