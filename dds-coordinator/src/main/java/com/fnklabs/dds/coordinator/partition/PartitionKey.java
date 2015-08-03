package com.fnklabs.dds.coordinator.partition;

import com.google.common.base.MoreObjects;
import org.jetbrains.annotations.NotNull;

import java.math.BigInteger;
import java.util.Objects;

public class PartitionKey implements Comparable<PartitionKey> {

    private final BigInteger tokenValue;

    public PartitionKey(BigInteger token) {
        tokenValue = token;
    }

    public BigInteger getTokenValue() {
        return tokenValue;
    }

    @Override
    public int compareTo(@NotNull PartitionKey o) {
        return getTokenValue().compareTo(o.getTokenValue());
    }

    @Override
    public int hashCode() {
        return getTokenValue().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof PartitionKey) {
            return Objects.equals(((PartitionKey) obj).getTokenValue(), getTokenValue());
        }

        return false;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this).add("value", getTokenValue()).toString();
    }
}
