package com.fnklabs.dds.coordinator;

import com.fnklabs.dds.coordinator.partition.PartitionTable;
import com.fnklabs.dds.network.ApiVersion;
import com.fnklabs.dds.network.ServerException;
import com.google.common.collect.Sets;
import com.google.common.net.HostAndPort;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.SettableFuture;
import org.jetbrains.annotations.NotNull;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;

import java.util.HashSet;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ServerNodeTest {

    @Before
    public void setUp() throws Exception {

    }

    @After
    public void tearDown() throws Exception {

    }

    /**
     * Check startup new cluster operation with normal flow
     *
     * @throws Exception
     * @throws ServerException
     */
    @Test
    public void testStartUpNewNode() throws Exception, ServerException {
        ServerNode serverNode = new ServerNode(HostAndPort.fromString("127.0.0.1:10000"), new Configuration() {
            @NotNull
            @Override
            public Set<HostAndPort> getSeeds() {
                return new HashSet<>();
            }


            @NotNull
            @Override
            public UUID getNodeId() {
                return UUID.randomUUID();
            }
        }, MoreExecutors.newDirectExecutorService(), mock(ServerNodeClientFactory.class));


        serverNode.onStartUp();

        Node.NodeStatus nodeStatus = serverNode.getNodeStatus();

        Assert.assertEquals(Node.NodeStatus.SETUP, nodeStatus);
    }

    /**
     * Check setUp operation with normal flow
     *
     * @throws Exception
     */
    @Test
    public void testSetUpInNewCluster() throws Exception {

        ClusterInformation clusterInformation = mock(ClusterInformation.class);
        when(clusterInformation.getCreated()).thenReturn(DateTime.now());

        SettableFuture<ClusterInformation> settableFuture = SettableFuture.<ClusterInformation>create();
        settableFuture.set(clusterInformation);

        ServerNodeClient serverNodeClient = mock(ServerNodeClient.class);
        when(serverNodeClient.nodeUp(any(NodeInfo.class))).thenReturn(settableFuture);


        ServerNodeClientFactory serverNodeClientFactory = mock(ServerNodeClientFactory.class);
        when(serverNodeClientFactory.getInstance(Matchers.any(HostAndPort.class))).thenReturn(serverNodeClient);


        ServerNode serverNode = new ServerNode(HostAndPort.fromString("127.0.0.1:10000"), new Configuration() {
            @NotNull
            @Override
            public Set<HostAndPort> getSeeds() {
                return Sets.newHashSet(HostAndPort.fromString("127.0.0.1:10001"));
            }

            @NotNull
            @Override
            public UUID getNodeId() {
                return UUID.randomUUID();
            }

        }, MoreExecutors.newDirectExecutorService(), serverNodeClientFactory);

        serverNode.onSetUp();

        Assert.assertEquals(Node.NodeStatus.START_UP, serverNode.getNodeStatus());

        serverNode.onStartUp();

        Assert.assertEquals(Node.NodeStatus.SETUP, serverNode.getNodeStatus());

        serverNode.onSetUp();

        Assert.assertEquals(Node.NodeStatus.UP, serverNode.getNodeStatus());
    }

    /**
     * Check setUp new node in the cluster if all seeds are not available
     *
     * @throws Exception
     */
    @Test()
    public void testSetUpCase2() throws Exception {


        ServerNode serverNode = new ServerNode(HostAndPort.fromString("127.0.0.1:10000"), new Configuration() {
            @NotNull
            @Override
            public Set<HostAndPort> getSeeds() {
                return Sets.newHashSet(HostAndPort.fromString("127.0.0.1:10001"));
            }

            @NotNull
            @Override
            public UUID getNodeId() {
                return UUID.randomUUID();
            }


        }, MoreExecutors.newDirectExecutorService(), new ServerNodeClientFactory());

        serverNode.onSetUp();

        Assert.assertEquals(Node.NodeStatus.START_UP, serverNode.getNodeStatus());

        serverNode.onStartUp();

        Assert.assertEquals(Node.NodeStatus.SETUP, serverNode.getNodeStatus());

        serverNode.onSetUp();

        Assert.assertEquals(Node.NodeStatus.SHUTDOWN, serverNode.getNodeStatus());
    }

    /**
     * Check setUp new node in the cluster if first seed is not available
     *
     * @throws Exception
     */
    @Test()
    public void testSetUpCase3() throws Exception {
        HostAndPort firstSeed = HostAndPort.fromString("127.0.0.1:10001");

        ClusterInformation clusterInformation = mock(ClusterInformation.class);
        when(clusterInformation.getCreated()).thenReturn(DateTime.now());


        SettableFuture<ClusterInformation> settableFuture = SettableFuture.<ClusterInformation>create();
        settableFuture.set(clusterInformation);

        ServerNodeClient firstNodeClient = mock(ServerNodeClient.class);
        when(firstNodeClient.nodeUp(any(NodeInfo.class))).thenReturn(settableFuture);


        ServerNodeClientFactory serverNodeClientFactory = mock(ServerNodeClientFactory.class);
        when(serverNodeClientFactory.getInstance(firstSeed)).thenReturn(firstNodeClient);

        ServerNode serverNode = new ServerNode(HostAndPort.fromString("127.0.0.1:10000"), new Configuration() {
            @NotNull
            @Override
            public Set<HostAndPort> getSeeds() {
                return Sets.newHashSet(firstSeed, HostAndPort.fromString("127.0.0.1:10002"));
            }

            @NotNull
            @Override
            public UUID getNodeId() {
                return UUID.randomUUID();
            }

        }, MoreExecutors.newDirectExecutorService(), serverNodeClientFactory);

        serverNode.onSetUp();

        Assert.assertEquals(Node.NodeStatus.START_UP, serverNode.getNodeStatus());

        serverNode.onStartUp();

        Assert.assertEquals(Node.NodeStatus.SETUP, serverNode.getNodeStatus());

        serverNode.onSetUp();

        Assert.assertEquals(Node.NodeStatus.UP, serverNode.getNodeStatus());
    }

    /**
     * Check setUp in new cluster operation with several members and normal flow
     * <p>
     * Awaiting repartiotion
     *
     * @throws Exception
     */
    @Test
    public void testNodeUp() throws Exception {

        HostAndPort firstNode = HostAndPort.fromString("127.0.0.1:10000");
        HostAndPort secondNode = HostAndPort.fromString("127.0.0.1:10001");
        HostAndPort thirdNode = HostAndPort.fromString("127.0.0.1:10002");


        NodeInfo firstNodeInfo = new NodeInfo(UUID.randomUUID(), firstNode, ApiVersion.VERSION_1.getVersion(), Node.NodeStatus.UP);
        NodeInfo secondNodeInfo = new NodeInfo(UUID.randomUUID(), secondNode, ApiVersion.VERSION_1.getVersion(), Node.NodeStatus.UP);
        NodeInfo thirdNodeInfo = new NodeInfo(UUID.randomUUID(), thirdNode, ApiVersion.VERSION_1.getVersion(), Node.NodeStatus.UP);

        ClusterInformation clusterInformation = mock(ClusterInformation.class);
        when(clusterInformation.getMembers()).thenReturn(new TreeSet<>(Sets.newHashSet(firstNodeInfo, secondNodeInfo)));
        when(clusterInformation.getCreated()).thenReturn(DateTime.now());
        when(clusterInformation.getPartitionTable()).thenReturn(mock(PartitionTable.class));

        SettableFuture<ClusterInformation> nodeUpFuture = SettableFuture.<ClusterInformation>create();
        nodeUpFuture.set(clusterInformation);
//
        ServerNodeClient serverNodeClient = mock(ServerNodeClient.class);
        when(serverNodeClient.nodeUp(any(NodeInfo.class))).thenReturn(nodeUpFuture);

//
        ServerNodeClientFactory serverNodeClientFactory = mock(ServerNodeClientFactory.class);
        when(serverNodeClientFactory.getInstance(Matchers.any(HostAndPort.class))).thenReturn(serverNodeClient);


        ServerNode serverNode = new ServerNode(HostAndPort.fromString("127.0.0.1:10000"), new Configuration() {
            @NotNull
            @Override
            public Set<HostAndPort> getSeeds() {
                return Sets.newHashSet(HostAndPort.fromString("127.0.0.1:10001"));
            }

            @NotNull
            @Override
            public UUID getNodeId() {
                return UUID.randomUUID();
            }

        }, MoreExecutors.newDirectExecutorService(), serverNodeClientFactory);


        serverNode.updateClusterInfo(clusterInformation);

        serverNode.onStartUp();

        Assert.assertEquals(Node.NodeStatus.SETUP, serverNode.getNodeStatus());

        serverNode.onSetUp();

        Assert.assertEquals(Node.NodeStatus.UP, serverNode.getNodeStatus());
        Assert.assertNotNull(serverNode.getClusterInfo().get());
        Assert.assertEquals(2, serverNode.getClusterInfo().get().getMembers().size());

        ListenableFuture<ClusterInformation> clusterInformationListenableFuture = serverNode.nodeUp(thirdNodeInfo);

        ClusterInformation clusterInformation1 = clusterInformationListenableFuture.get();

        Assert.assertNotNull(clusterInformation1);

        Assert.assertEquals(Node.NodeStatus.REPAIR, serverNode.getNodeStatus());

        Assert.assertEquals(3, serverNode.getClusterInfo().get().getMembers().size());

        Assert.assertEquals(ClusterStatus.REPAIR, serverNode.getClusterStatus());
    }

    @Test
    public void testRepair() throws Exception {
        ClusterInformation clusterInformation = mock(ClusterInformation.class);
        when(clusterInformation.getCreated()).thenReturn(DateTime.now());

        ServerNode serverNode = new ServerNode(HostAndPort.fromString("127.0.0.1:10000"), mock(Configuration.class), MoreExecutors.sameThreadExecutor(), mock(ServerNodeClientFactory.class));
        ListenableFuture<Boolean> booleanListenableFuture = serverNode.updateClusterInfo(clusterInformation);

        Assert.assertTrue(booleanListenableFuture.get());

        Thread.sleep(100);

        ClusterInformation newClusterInformation = mock(ClusterInformation.class);
        when(newClusterInformation.getCreated()).thenReturn(DateTime.now());


        ListenableFuture<Boolean> repair = serverNode.repair(newClusterInformation);


        Assert.assertTrue(repair.get(1, TimeUnit.SECONDS));
        Assert.assertEquals(Node.NodeStatus.REPAIR, serverNode.getNodeStatus());


        newClusterInformation = mock(ClusterInformation.class);
        when(newClusterInformation.getCreated()).thenReturn(DateTime.now().minusHours(1));


        repair = serverNode.repair(newClusterInformation);


        Assert.assertTrue(repair.get(1, TimeUnit.SECONDS));
    }


    @Test
    public void testOnRepair() throws Exception {

    }
}