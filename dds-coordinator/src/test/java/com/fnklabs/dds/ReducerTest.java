package com.fnklabs.dds;

import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

@Ignore
public class ReducerTest {

    @Test
    @Ignore
    public void testReducer() throws Exception {
        List<Integer> list = Arrays.asList(1, 1, 1, 1, 1, 1, 1, 1);

        list.stream().reduce((a, b) -> {
            LoggerFactory.getLogger(getClass()).debug("{} {}", a, b);
            return a * b;
        });

    }
}
