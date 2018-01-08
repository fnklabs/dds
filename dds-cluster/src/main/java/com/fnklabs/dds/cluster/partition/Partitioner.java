package com.fnklabs.dds.cluster.partition;

import com.fnklabs.dds.cluster.Node;
import com.fnklabs.dds.cluster.partition.exception.RepartitionIllegalOperation;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.stream.Collectors;

/**
 * Partitioner
 */
public class Partitioner {
    /**
     * Min token value
     */
    public final static long MIN_TOKEN_VALUE = Long.MIN_VALUE;

    /**
     * Max token value
     */
    public final static long MAX_TOKEN_VALUE = Long.MAX_VALUE;

    public static final int PARTITIONS_PER_NODE = 256;
    /**
     * Record key hash function
     */
    private static final HashFunction HASH_FUNCTION = Hashing.murmur3_128();

    /**
     * Build partition key
     *
     * @param key
     *
     * @return
     */
    public static long buildPartitionKey(byte[] key) {
        return hash(key);
    }

    /**
     * Build partition table
     *
     * @param numberOfPartitions Number of partitions in the cluster
     *
     * @return Set of partition
     */
    public static Set<Partition> split(int numberOfPartitions) {
        long step = MAX_TOKEN_VALUE / 2 / numberOfPartitions;

        Set<Partition> partitions = new HashSet<>();

        long leftBorder = MIN_TOKEN_VALUE;

        for (int i = 0; i < numberOfPartitions; i++) {
            long rightBorder = leftBorder + step;

            Partition partition = new PartitionImpl(leftBorder, rightBorder, PartitionState.BALANCING);

            partitions.add(partition);

            leftBorder = rightBorder;
        }

        return partitions;
    }

    /**
     * Build partition table
     *
     * @param members           Cluster members
     * @param replicationFactor Replication factor
     *
     * @return Partition table
     */
    public static PartitionTable buildPartitionTable(final SortedSet<Node> members, final int replicationFactor) throws RepartitionIllegalOperation {
        if (members.size() < replicationFactor) {
            throw new RepartitionIllegalOperation(replicationFactor, members.size());
        }

        Set<Partition> partitionSet = split(members.size() * PARTITIONS_PER_NODE);

        PartitionTableImpl partitionTable = new PartitionTableImpl();

        for (int memberIndex = 0; memberIndex < members.size(); memberIndex++) {
            int offset = PARTITIONS_PER_NODE * memberIndex;

            Set<Partition> partitionsForMember = partitionSet.stream()
                                                             .skip(offset)
                                                             .limit(PARTITIONS_PER_NODE)
                                                             .collect(Collectors.toSet());

            for (Partition partition : partitionsForMember) {

                for (int replicationFactorIndex = 0; replicationFactorIndex < replicationFactor; replicationFactorIndex++) {
                    int partitionOwnerIndex = (memberIndex + replicationFactorIndex) % members.size();

                    Node partitionOwner = new ArrayList<>(members).get(partitionOwnerIndex);

                    partitionTable.addPartition(partition, partitionOwner);
                }
            }
        }


        return partitionTable;
    }

    /**
     * Get internal id of user key by hashing user key
     *
     * @param key User key
     *
     * @return Internal key
     */
    private static long hash(byte[] key) {
        HashCode hashCode = HASH_FUNCTION.hashBytes(key);
        return hashCode.asLong();
    }


}
