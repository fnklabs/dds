package com.fnklabs.dds.storage.im;

import com.fnklabs.dds.storage.ScanFunction;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.BitSet;

public class ImTableStorageTest {

    public static final int MAX_SIZE = 128 * 1024 * 1024; // 128 MB
    private ImTableStorage imStorage;

    private byte[] data;

    private byte[] buffer;

    @Before
    public void setUp() throws Exception {
        imStorage = new ImTableStorage(MAX_SIZE, 128); // 8 MB

        data = new byte[]{
                0, 0, 0, 0, 0, 0, 0, 1,
                0, 0, 0, 0, 0, 0, 0, 2,
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

        for (int i = 0; i < MAX_SIZE / data.length; i++) {
            imStorage.write(i * data.length, data);
        }

        imStorage.scan(
                0,
                MAX_SIZE,
                new ScanFunction() {
                    @Override
                    public boolean accept(long position, byte[] data) {
                        String msg = String.format(
                                "error on %d expected %s actual %s",
                                position,
                                BitSet.valueOf(ImTableStorageTest.this.data).toString(),
                                BitSet.valueOf(data).toString()
                        );

                        Assert.assertEquals(ImTableStorageTest.this.data.length, data.length);
                        Assert.assertArrayEquals(msg, ImTableStorageTest.this.data, data);

                        return true;
                    }
                },
                () -> new byte[data.length]
        );


    }


}