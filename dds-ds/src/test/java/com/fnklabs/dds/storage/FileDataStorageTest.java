package com.fnklabs.dds.storage;

import com.fnklabs.dds.DdsVersion;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class FileDataStorageTest {
    private int dataSize;
    private Path path;
    private MetaInformation metaInformation;
    private FileDataStorage fileDataStorage;

    @Before
    public void setUp() throws Exception {
        path = new File("file.dds").toPath();

        metaInformation = new MetaInformation(
                DdsVersion.CURRENT,
                new UUID(0, 1),
                Integer.BYTES,
                0,
                0
        );

        dataSize = DataBlock.Header.HEADER_SIZE + metaInformation.getKeyLength() + Integer.BYTES;
    }

    @After
    public void tearDown() throws Exception {
        path.toFile().delete();
    }

    @Test
    public void open() throws Exception {
        assertEquals(0, path.toFile().length());

        fileDataStorage = FileDataStorage.open(path, metaInformation);

        assertEquals(MetaInformation.DEFAULT_SIZE, path.toFile().length());
    }

    @Test
    public void readNotExistingValue() throws Exception {
        fileDataStorage = FileDataStorage.open(path, metaInformation);

        DataBlock read = fileDataStorage.get(new byte[]{0, 0, 0, 1});

        assertNull(read);
    }

    @Test
    public void readExistingValue() throws IOException {
        fileDataStorage = FileDataStorage.open(path, metaInformation);

        byte[] key = {0, 0, 0, 1};
        byte[] data = {0, 0, 0, 2};

        fileDataStorage.put(key, data);

        DataBlock dataBlock = fileDataStorage.get(key);

        assertNotNull(dataBlock);

        assertArrayEquals(String.format("%s", dataBlock.getKey()), key, dataBlock.getKey());
        assertArrayEquals(String.format("%s", dataBlock.getData()), data, dataBlock.getData());
    }

    @Test
    public void write() throws Exception {
        fileDataStorage = FileDataStorage.open(path, metaInformation);

        long position = fileDataStorage.put(new byte[]{0, 0, 0, 1}, new byte[]{0, 0, 0, 1});

        assertEquals(FileDataStorage.DATA_START_OFFSET, position);

        position = fileDataStorage.put(new byte[]{0, 0, 0, 2}, new byte[]{0, 0, 0, 1});

        assertEquals(FileDataStorage.DATA_START_OFFSET + dataSize, position);
    }

    @Test
    public void scan() throws Exception {
        fileDataStorage = FileDataStorage.open(path, metaInformation);

        for (int i = 1; i < 5; i++) {
            ByteBuffer key = ByteBuffer.allocate(Integer.BYTES);
            key.putInt(i);

            ByteBuffer data = ByteBuffer.allocate(Integer.BYTES);
            data.putInt(i);

            fileDataStorage.put(key.array(), data.array());
        }

        AtomicInteger count = new AtomicInteger();

        fileDataStorage.scan(
                (position, dataBlock) -> {
                    ByteBuffer buffer = ByteBuffer.wrap(dataBlock.getData());

                    int value = buffer.getInt();

                    return value % 2 == 0;
                },
                (position, data) -> {
                    count.incrementAndGet();

                    return true;
                });

        assertEquals(2, count.intValue());
    }
}