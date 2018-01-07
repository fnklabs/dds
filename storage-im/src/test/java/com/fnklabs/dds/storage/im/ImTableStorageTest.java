package com.fnklabs.dds.storage.im;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ImTableStorageTest {

    private ImTableStorage imStorage;

    private byte[] data;

    private byte[] buffer;

    @Before
    public void setUp() throws Exception {
        imStorage = new ImTableStorage(64 * 1024 * 1024, 128, 8 * 1024 * 1024); // 64 MB

        data = new byte[]{
                0, 0, 0, 0, 0, 0, 0, 1,
                0, 0, 0, 0, 0, 0, 0, 2,
                0, 0, 0, 3,
        };

        buffer = new byte[data.length];
    }

    @Test
    public void write() {
        imStorage.write(0, data);

        imStorage.read(0, buffer);


        Assert.assertArrayEquals(data, buffer);
    }

    @Test
    public void readNone() {
        imStorage.read(0, buffer);

        Assert.assertArrayEquals(new byte[buffer.length], buffer);
    }

    @Test
    public void readInt() {
        byte[] intData = new byte[4];

        imStorage.read(0, intData);


        Assert.assertArrayEquals(new byte[4], intData);
    }

    @Test
    public void scan() {

        for (int i = 0; i < 10; i++) {
            imStorage.write(i * data.length, data);
        }

        for (int i = 0; i < 10; i++) {
            imStorage.read(i * data.length, buffer);

            Assert.assertArrayEquals(data, buffer);
        }
    }


}