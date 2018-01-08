package com.fnklabs.dds.index;

import com.fnklabs.dds.BytesUtils;
import com.google.common.base.MoreObjects;
import org.jetbrains.annotations.NotNull;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Objects;
import java.util.Optional;


class Node implements Comparable<Node> {

    /**
     * Key
     */
    private final byte[] key;

    /**
     * Position in index file
     */
    private final Long position;

    /**
     * Reference on data
     */
    private final Long dataReference;

    private final long depth;

    private final Long leftNodeReference;
    private final Long rightNodeReference;


    private Node(byte[] key, Long position, Long dataReference, long depth, Long leftNodeReference, Long rightNodeReference) {
        this.key = key;
        this.position = position;
        this.dataReference = dataReference;
        this.depth = depth;
        this.leftNodeReference = leftNodeReference;
        this.rightNodeReference = rightNodeReference;
    }

    public static Builder builder() {
        return new Builder();
    }


    public long getDepth() {
        return depth;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                          .add("key", new BigInteger(key))
                          .add("position", getPosition().orElse(-1L))
                          .add("dataRef", getDataReference().orElse(-1L))
                          .add("depth", getDepth())
                          .add("leftNode", getLeftNodeReference().orElse(-1L))
                          .add("rightNode", getRightNodeReference().orElse(-1L))
                          .toString();
    }

    public byte[] getKey() {
        return key;
    }

    public Optional<Long> getPosition() {
        return Optional.ofNullable(position);
    }

    public Optional<Long> getDataReference() {
        return Optional.ofNullable(dataReference);
    }

    public Optional<Long> getLeftNodeReference() {
        return Optional.ofNullable(leftNodeReference);
    }

    public Optional<Long> getRightNodeReference() {
        return Optional.ofNullable(rightNodeReference);
    }

    public boolean hasChild() {
        return getLeftNodeReference().isPresent() || getRightNodeReference().isPresent();
    }

    public boolean hasLessThanTwoChild() {
        return leftNodeReference == null || rightNodeReference == null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int compareTo(@NotNull Node o) {
        return BytesUtils.compare(getKey(), o.getKey());
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(getKey());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof Node) {
            return Arrays.equals(((Node) obj).getKey(), getKey());
        }
        return false;
    }

    static final class Builder {
        private byte[] key;
        private Long position;
        private Long dataReference;
        private long depth = 0;
        private Long leftNodeReference;
        private Long rightNodeReference;

        Node build() {
            return new Node(key, position, dataReference, depth, leftNodeReference, rightNodeReference);
        }

        Builder withKey(byte[] key) {
            this.key = key;
            return this;
        }

        Builder withPosition(Long position) {
            this.position = position;
            return this;
        }

        Builder withDataReference(Long dataReference) {
            this.dataReference = dataReference;
            return this;
        }

        Builder withDepth(long depth) {
            this.depth = depth;
            return this;
        }

        Builder withLeftNodeReference(Long reference) {
            this.leftNodeReference = reference;
            return this;
        }

        Builder withRightNodeReference(Long reference) {
            this.rightNodeReference = reference;
            return this;
        }

        Builder from(Node nodeItem) {
            this.key = nodeItem.getKey();
            this.position = nodeItem.getPosition().orElse(null);
            this.dataReference = nodeItem.getDataReference().orElse(null);
            this.depth = nodeItem.getDepth();
            this.leftNodeReference = nodeItem.getLeftNodeReference().orElse(null);
            this.rightNodeReference = nodeItem.getRightNodeReference().orElse(null);

            return this;
        }
    }
}
