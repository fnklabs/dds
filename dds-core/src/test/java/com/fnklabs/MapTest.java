package com.fnklabs;

import org.junit.*;
import org.slf4j.*;

import java.util.*;
import java.util.concurrent.*;

public class MapTest {
    @Test
    public void mapSize() throws Exception {
        int h;
        Integer val = new Integer(10_000_000);
        int i = (h = val.hashCode()) ^ (h >>> 16);

        LoggerFactory.getLogger(getClass()).debug("Hash: {}", i);

    }
}
