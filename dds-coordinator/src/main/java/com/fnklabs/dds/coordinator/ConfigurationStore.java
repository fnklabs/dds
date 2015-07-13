package com.fnklabs.dds.coordinator;

import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.concurrent.locks.ReentrantLock;

class ConfigurationStore {

    public static final String NODE_INFO_DDS_FILENAME = "data/node_info.dds";

    private static ReentrantLock lock = new ReentrantLock(false);


    public static RingInfo read() {
        lock.lock();
        try {
            ObjectInputStream inputStream = new ObjectInputStream(new FileInputStream(NODE_INFO_DDS_FILENAME));
            RingInfo nodeInfo = (RingInfo) inputStream.readObject();
            inputStream.close();
            return nodeInfo;
        } catch (IOException | ClassNotFoundException e) {
            LoggerFactory.getLogger(ConfigurationStore.class).warn("Can't read node configuration", e);

            return null;
        } finally {

            lock.unlock();

        }

    }

    public static void update(RingInfo nodeInfo) {
        lock.lock();

        try {
            ObjectOutputStream objectOutputStream = new ObjectOutputStream(new FileOutputStream(NODE_INFO_DDS_FILENAME));
            objectOutputStream.writeObject(nodeInfo);
            objectOutputStream.close();
        } catch (IOException e) {
            LoggerFactory.getLogger(ConfigurationStore.class).warn("Can't update node configuration", e);
        } finally {
            lock.unlock();
        }
    }


}
