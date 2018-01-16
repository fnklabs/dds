package com.fnklabs.dds.storage.columnar;

import com.fnklabs.dds.table.Expression;
import com.google.common.collect.Range;
import com.google.common.hash.BloomFilter;

class Statistic {
    private final BloomFilter<byte[]> bloomFilter;

    private final Range<Long> position;

    private final int size;

    Statistic(int size, long endPosition, long beginPosition) {
        this(10_000, 0.01f, size, beginPosition, endPosition);
    }

    Statistic(long insertions, float fpp, int size, long beginPosition, long endPosition) {
        this.bloomFilter = BloomFilter.create((from, into) -> into.putBytes(from), insertions, fpp);
        this.position = Range.closedOpen(beginPosition, endPosition);
        this.size = size;
    }

    /**
     * Check expression by current statistic
     * <p>
     * Supported only {@link Expression#EQ} and {@link Expression#NEQ}
     *
     * @param expression Query clause expression
     * @param value      Clause value
     *
     * @return True if not match False otherwise
     */
    public boolean match(Expression expression, byte[] value) {
        if (expression == Expression.EQ) {
            return bloomFilter.mightContain(value);
        }

        return true;
    }

    public Range<Long> getRange() {
        return position;
    }

    public long startPosition() {
        return position.lowerEndpoint();
    }

    public int getSize() {
        return size;
    }

    public void addValue(byte[] data) {
        bloomFilter.put(data);
    }
}
