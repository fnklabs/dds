package com.fnklabs.dds.coordinator;

import com.google.common.collect.Sets;
import com.google.common.net.HostAndPort;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Files;
import java.nio.file.Paths;

public class ConfigurationStoreTest {

    @Before
    public void setUp() throws Exception {
        if (Files.exists(Paths.get(ConfigurationStore.NODE_INFO_DDS_FILENAME))) {
            Files.delete(Paths.get(ConfigurationStore.NODE_INFO_DDS_FILENAME));
        }
    }

    @After
    public void tearDown() throws Exception {
        if (Files.exists(Paths.get(ConfigurationStore.NODE_INFO_DDS_FILENAME))) {
            Files.delete(Paths.get(ConfigurationStore.NODE_INFO_DDS_FILENAME));
        }
    }

    @Test
    public void testRead() throws Exception {
        ConfigurationStore configurationStore = new ConfigurationStore();

        RingInfo read = configurationStore.read();

        Assert.assertNull(read);


    }

    @Test
    public void testDumpAndRead() throws Exception {
        ConfigurationStore configurationStore = new ConfigurationStore();

        NodeInfo nodeInfo = new NodeInfo(HostAndPort.fromParts("127.0.0.1", 10000), "1");
        RingInfo ringInfo = new RingInfo(nodeInfo, Ring.sort(Sets.newHashSet(nodeInfo)), nodeInfo);

        configurationStore.update(ringInfo);

        RingInfo read = configurationStore.read();

        Assert.assertNotNull(read);

        Assert.assertEquals(ringInfo.getCoordinator(), read.getCoordinator());
        Assert.assertEquals(ringInfo.getCreated(), read.getCreated());
    }
}