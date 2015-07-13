package com.fnklabs.dds.network;

import com.fnklabs.dds.coordinator.DistributedDataSet;
import com.fnklabs.dds.TestObjectHelper;
import org.junit.*;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Ignore
public class ClientTest {
    public DistributedDataSet<TestObjectHelper> dds;
    private ThreadPoolTaskExecutor threadPoolTaskExecutor;

//    @Before
//    public void setUp() throws IOException {
//
//        threadPoolTaskExecutor = new ThreadPoolTaskExecutor();
//        threadPoolTaskExecutor.setCorePoolSize(4);
//        threadPoolTaskExecutor.setMaxPoolSize(4);
//        threadPoolTaskExecutor.setQueueCapacity(60000);
//        threadPoolTaskExecutor.setThreadNamePrefix("TestWorker-");
//        threadPoolTaskExecutor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
//        threadPoolTaskExecutor.setWaitForTasksToCompleteOnShutdown(true);
//        threadPoolTaskExecutor.setAwaitTerminationSeconds(360);
//        threadPoolTaskExecutor.initialize();
//    }
//
//    @After
//    public void tearDown() throws IOException {
//    }
//
//    @Test
//    public void testSend() throws IOException, ExecutionException, InterruptedException {
//        Server server = Server.create("127.0.0.1", 50000);
//        server
//                .register(new PingMessageHandler())
//                .start();
//
//        Client client = Client.create("127.0.0.1", 50000, new Consumer<ConnectorMessageBuffer>() {
//            @Override
//            public void accept(ConnectorMessageBuffer response) {
//                LoggerFactory.getLogger(ClientTest.class).debug("Retrieved result: {}", response.getId());
//            }
//        }).build();
//        ByteBuffer msg = ByteBuffer.allocate(Integer.BYTES);
//        msg.putInt(1);
//        ListenableFuture<ConnectorMessageBuffer> future = client.send(OperationType.CLUSTER_INFO, msg);
//
//        Futures.addCallback(future, new FutureCallback<ConnectorMessageBuffer>() {
//            @Override
//            public void onSuccess(ConnectorMessageBuffer result) {
//                LoggerFactory.getLogger(getClass()).debug("Retrieved result: {}/{}", result.getId(), result.getMessageSize());
//            }
//
//            @Override
//            public void onFailure(Throwable t) {
//                LoggerFactory.getLogger(getClass()).warn("Cant retrieve result", t);
//            }
//        });
//
//        ListenableFuture<ConnectorMessageBuffer> transform = Futures.transform(future, (ConnectorMessageBuffer response) -> response, threadPoolTaskExecutor.getThreadPoolExecutor());
//
//        while (threadPoolTaskExecutor.getActiveCount() > 0) {
//
//        }
//
//        ConnectorMessageBuffer connectorMessageBuffer = transform.get();
//
//        Assert.assertNotNull(connectorMessageBuffer);
//
//        server.stop();
//        client.close();
//        Client.shutdown();
//    }
//
//
//    @Test
//    public void testSendAsync() throws Exception {
//        Server server = Server.create("127.0.0.1", 50000);
//        server
//                .register(new PingMessageHandler())
//                .start();
//
//        List<Client> clientList = new ArrayList<>();
//
//        for (int i = 0; i < 5; i++) {
//            Client client = Client.create("127.0.0.1", 50000, response -> {
////                Long executionTime = response.<Long>unpack();
//
////                LoggerFactory.getLogger(getClass()).debug("Execution time: {} msec", executionTime);
//            }).build();
//
//            clientList.add(client);
//        }
//
//        List<ListenableFuture<ConnectorMessageBuffer>> futures = Collections.synchronizedList(new ArrayList<>());
//
//        StopWatch stopWatch = new StopWatch("network:operations");
//
//        int operations = 10000;
//
//        AtomicLong completedOperations = new AtomicLong(0);
//
//
//        for (int i = 0; i < operations; i++) {
//            ByteBuffer msg = ByteBuffer.allocate(Integer.BYTES);
//            msg.putInt(i);
//
//            clientList.forEach(client -> {
//                threadPoolTaskExecutor.submit(() -> {
//
//
//                    ListenableFuture<ConnectorMessageBuffer> future = client.send(OperationType.PING, System.currentTimeMillis());
//
//
//                    futures.add(future);
//
//                    Futures.addCallback(future, new FutureCallback<ConnectorMessageBuffer>() {
//                        @Override
//                        public void onSuccess(ConnectorMessageBuffer result) {
////                            Long executionTime = result.<Long>unpack();
//
////                            LoggerFactory.getLogger(getClass()).debug("Execution time: {} msec", executionTime);
//
//                            long l = completedOperations.incrementAndGet();
//
////                            LoggerFactory.getLogger(getClass()).debug("Completed operations: {}/{}", l, futures.size());
//                        }
//
//                        @Override
//                        public void onFailure(Throwable t) {
////                            LoggerFactory.getLogger(getClass()).warn("Cant execute ", t);
//                        }
//                    }, threadPoolTaskExecutor);
//
//
//                });
//            });
//        }
//
//
//        Thread.sleep(1000);
//
//        while (threadPoolTaskExecutor.getActiveCount() > 0) {
//            LoggerFactory.getLogger(getClass()).debug("Active tasks: {}", threadPoolTaskExecutor.getThreadPoolExecutor().getQueue().size());
//
//            Thread.sleep(100);
//        }
//        LoggerFactory.getLogger(getClass()).debug("As list: {}", futures.size());
//        ListenableFuture<List<ConnectorMessageBuffer>> resultFutures = Futures.allAsList(futures);
//
//
//        LoggerFactory.getLogger(getClass()).debug("Waiting futures: {}", futures.size());
//
//        Thread.sleep(5000);
//
//        List<ConnectorMessageBuffer> connectorMessageBufferList = Futures.get(resultFutures, Exception.class);
//        Assert.assertNotNull(connectorMessageBufferList);
//
//        long successResponseCount = connectorMessageBufferList.parallelStream().filter(response -> response.isOk()).count();
//        long failedResponseCount = connectorMessageBufferList.size() - successResponseCount;
//
//
//        LoggerFactory.getLogger(ClientTest.class).debug("Completed operations: {} Success: {} Failed: {}", stopWatch.toString(operations * clientList.size()), successResponseCount, failedResponseCount);
//
//        server.stop();
//
//        clientList.forEach(Client::close);
//
//        Client.shutdown();
//    }
}