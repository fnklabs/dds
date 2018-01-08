package com.fnklabs.dds.cluster;

import java.util.SortedSet;
import java.util.function.Function;

public interface ConsistencyFunction {
    <T> T execute(SortedSet<Node> members, Function<Node, T> function) throws InconsistientException;
}
