package com.fnklabs.dds.coordinator.operation;

import com.fnklabs.dds.coordinator.ConsistencyLevel;
import com.fnklabs.dds.coordinator.LoadBalancingPolicy;
import com.fnklabs.dds.coordinator.NodeInfo;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

/**
 * Distributed operation in the cluster that must achieve some consistency level
 */
abstract class DistributedOperation implements Operation {

    /**
     * Execute operation
     *
     * @param callback Callback that must retrieve {@link NodeInfo} host for operation execution and return ListenableFuture for response
     *
     * @return Future for request execution
     */
    public static ListenableFuture<Boolean> execute(OperationOptions options, Function<NodeInfo, ListenableFuture<Boolean>> callback) {
        ConsistencyLevel consistencyLevel = options.getConsistencyLevel();

        int retryCount = options.getRetryCount();

        LoadBalancingPolicy loadBalancingPolicy = options.getLoadBalancingPolicy();


        List<NodeInfo> nodes = loadBalancingPolicy.getExecutionPlan();

        List<ListenableFuture<Boolean>> resultList = new ArrayList<>();

        nodes.forEach(nodeInfo -> {
            AtomicInteger operationRetryCount = new AtomicInteger(retryCount);

            ListenableFuture<Boolean> booleanListenableFuture = executeCallback(callback, nodeInfo, operationRetryCount);

            resultList.add(booleanListenableFuture);
        });

        ListenableFuture<List<Boolean>> resultFutures = Futures.allAsList(resultList);

        if (consistencyLevel == ConsistencyLevel.ALL) {

            return Futures.transform(resultFutures, (List<Boolean> status) -> {
                return !status.contains(false);
            }, MoreExecutors.directExecutor());

        } else if (consistencyLevel == ConsistencyLevel.QUORUM) {
            return Futures.transform(resultFutures, (List<Boolean> statusList) -> {
                int minNumberOfSuccessfulOperations = statusList.size() / 2 + 1;

                long successOperations = statusList.stream().filter(status -> status).count();

                return successOperations >= minNumberOfSuccessfulOperations;
            }, MoreExecutors.sameThreadExecutor());

        } else if (consistencyLevel == ConsistencyLevel.ONE) {
            return Futures.transform(resultFutures, (List<Boolean> statusList) -> {
                return statusList.contains(true);
            }, MoreExecutors.sameThreadExecutor());
        }

        return null;
    }

    /**
     * Execute user user function with retry count
     *
     * @param userFunction User function
     * @param nodeInfo     Node info for user user function execution
     * @param retryCount   Available retry count for function execution
     *
     * @return User function execution response (true on success false on error)
     */
    private static ListenableFuture<Boolean> executeCallback(Function<NodeInfo, ListenableFuture<Boolean>> userFunction, NodeInfo nodeInfo, AtomicInteger retryCount) {

        if (retryCount.get() <= 0) {
            SettableFuture<Boolean> falseFuture = SettableFuture.<Boolean>create();
            falseFuture.set(false);
            return falseFuture;
        }

        return Futures.transform(userFunction.apply(nodeInfo), (Boolean response) -> {

            if (response) {
                SettableFuture<Boolean> successFuture = SettableFuture.<Boolean>create();
                successFuture.set(true);
                return successFuture;
            }

            retryCount.decrementAndGet();

            return executeCallback(userFunction, nodeInfo, retryCount);
        }, MoreExecutors.sameThreadExecutor());
    }
}
