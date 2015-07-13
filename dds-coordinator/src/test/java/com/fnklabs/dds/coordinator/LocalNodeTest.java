package com.fnklabs.dds.coordinator;

import org.junit.Test;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

public class LocalNodeTest {

    @Test
    public void testRead() throws Exception {
        File ddsPath = Paths.get("test").toFile();
        if (!ddsPath.exists()) {
            ddsPath.createNewFile();
        }

        ObjectInputStream objectInputStream = new ObjectInputStream(new FileInputStream(ddsPath));

        while (true) {
            TestObject o = (TestObject) objectInputStream.readObject();

            LoggerFactory.getLogger(getClass()).debug("Object: {}", o);
        }
    }

    @Test
    public void testWrite() throws Exception {
        File ddsPath = Paths.get("test").toFile();
        if (!ddsPath.exists()) {
            ddsPath.createNewFile();
        }

        List<TestObject> testList = Arrays.asList(new TestObject(), new TestObject(), new TestObject());

        ObjectOutputStream objectOutputStream = new ObjectOutputStream(new FileOutputStream(ddsPath, true));

        testList.forEach(test -> {
            try {
                objectOutputStream.writeObject(test);

                LoggerFactory.getLogger(getClass()).debug("Object: {}", test);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
    }


}