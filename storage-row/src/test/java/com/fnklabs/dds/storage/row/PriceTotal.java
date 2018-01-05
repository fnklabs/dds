package com.fnklabs.dds.storage.row;

import com.fnklabs.dds.storage.Mapper;
import com.fnklabs.dds.storage.Reducer;

import java.util.Collection;
import java.util.function.Consumer;

public class PriceTotal implements Mapper<RowChunk, Integer> {
    @Override
    public void map(RowChunk chunk, Consumer<Integer> consumer) {

    }

    static class SumPrice implements Reducer<Integer, Integer> {

        @Override
        public Integer reduce(Collection<Integer> items) {
            return items.stream()
                        .reduce(Integer::sum)
                        .orElse(0);
        }
    }
}
