package com.fnklabs.dds.cluster;

import org.slf4j.Logger;

import java.util.SortedSet;
import java.util.function.Function;

import static org.slf4j.LoggerFactory.getLogger;


class ConsistencyOneExecutor implements ConsistencyFunction {
    private static final Logger log = getLogger(ConsistencyOneExecutor.class);

    @Override
    public <T> T execute(SortedSet<Node> members, Function<Node, T> function) throws InconsistientException {
        for (Node member : members) {
            try {
                return function.apply(member);
            } catch (Exception e) {
                log.warn("can't execute function", e);
            }
        }

        throw new InconsistientException();
    }
}
