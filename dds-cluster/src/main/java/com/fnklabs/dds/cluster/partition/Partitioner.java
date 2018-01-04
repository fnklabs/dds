package com.fnklabs.dds.coordinator.partition;

import com.fnklabs.dds.coordinator.partition.exception.RepartitionIllegalOperation;
import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.google.common.net.HostAndPort;

import java.math.BigInteger;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Partitioner
 */
public class Partitioner {
    /**
     * Min token value
     */
    public final static byte[] MIN_TOKEN_VALUE = new byte[]{
            Byte.MIN_VALUE, Byte.MIN_VALUE, Byte.MIN_VALUE, Byte.MIN_VALUE,
            Byte.MIN_VALUE, Byte.MIN_VALUE, Byte.MIN_VALUE, Byte.MIN_VALUE,
            Byte.MIN_VALUE, Byte.MIN_VALUE, Byte.MIN_VALUE, Byte.MIN_VALUE,
            Byte.MIN_VALUE, Byte.MIN_VALUE, Byte.MIN_VALUE, Byte.MIN_VALUE,
    };

    /**
     * Max token value
     */
    public final static byte[] MAX_TOKEN_VALUE = new byte[]{
            Byte.MAX_VALUE, Byte.MAX_VALUE, Byte.MAX_VALUE, Byte.MAX_VALUE,
            Byte.MAX_VALUE, Byte.MAX_VALUE, Byte.MAX_VALUE, Byte.MAX_VALUE,
            Byte.MAX_VALUE, Byte.MAX_VALUE, Byte.MAX_VALUE, Byte.MAX_VALUE,
            Byte.MAX_VALUE, Byte.MAX_VALUE, Byte.MAX_VALUE, Byte.MAX_VALUE,
    };

    /**
     * Token length in bytes
     */
    public final static int TOKEN_LENGTH = 16; // 128 bit or 16 byte

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
    public static PartitionKey buildPartitionKey(byte[] key) {
        byte[] token = hash(key);

        return new PartitionKey(new BigInteger(token));
    }

    /**
     * Build partition table
     *
     * @param numberOfPartitions Number of partitions in the cluster
     *
     * @return Set of partition
     */
    public static Set<Partition> split(int numberOfPartitions) {
        BigInteger step = new BigInteger(MAX_TOKEN_VALUE).shiftLeft(1).divide(BigInteger.valueOf(numberOfPartitions));
        BigInteger maxValue = new BigInteger(MAX_TOKEN_VALUE);

        Set<Partition> partitions = new HashSet<>();

        BigInteger leftBorder = new BigInteger(MIN_TOKEN_VALUE);

        for (int i = 0; i < numberOfPartitions; i++) {
            BigInteger rightBorder = leftBorder.add(step);//.subtract(BigInteger.ONE);
            Partition partition;

            if (rightBorder.compareTo(maxValue) != 0) {
                partition = new Partition(new PartitionKey(leftBorder), new PartitionKey(rightBorder.subtract(BigInteger.ONE)), Partition.State.BALANCING);
            } else {
                partition = new Partition(new PartitionKey(leftBorder), new PartitionKey(rightBorder), Partition.State.BALANCING);
            }

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
    public static PartitionTable buildPartitionTable(final SortedSet<UUID> members, final int replicationFactor) throws RepartitionIllegalOperation {
        if (members.size() < replicationFactor) {
            throw new RepartitionIllegalOperation(replicationFactor, members.size());
        }

        Set<Partition> partitionSet = split(members.size() * PARTITIONS_PER_NODE);

        PartitionTable partitionTable = new PartitionTable();

        for (int memberIndex = 0; memberIndex < members.size(); memberIndex++) {
            int offset = PARTITIONS_PER_NODE * memberIndex;
            Set<Partition> partitionsForMember = partitionSet.stream().skip(offset).limit(PARTITIONS_PER_NODE).collect(Collectors.toSet());

            for (Partition partition : partitionsForMember) {

                for (int replicationFactorIndex = 0; replicationFactorIndex < replicationFactor; replicationFactorIndex++) {
                    int partitionOwnerIndex = (memberIndex + replicationFactorIndex) % members.size();

                    UUID partitionOwner = new ArrayList<>(members).get(partitionOwnerIndex);

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
    protected static byte[] hash(byte[] key) {
        HashCode hashCode = HASH_FUNCTION.hashBytes(key);
        return hashCode.asBytes();
    }


}
