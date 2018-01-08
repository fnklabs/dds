package com.fnklabs.dds.storage;


import com.fnklabs.dds.DdsVersion;
import com.fnklabs.metrics.MetricsFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

public class RewriteStorageTest {
    private Path path;
    private FileDataStorage fileDataStorage;

    @Before
    public void setUp() throws Exception {
        path = new File("file.dds").toPath();

        MetaInformation metaInformation = new MetaInformation(
                DdsVersion.CURRENT,
                new UUID(0, 1),
                Integer.BYTES,
                0,
                0
        );

        fileDataStorage = FileDataStorage.open(path, metaInformation);
    }

    @After
    public void tearDown() throws Exception {
        path.toFile().delete();

        MetricsFactory.getMetrics().report();
    }

    @Test
    public void testRewriteOperation() throws IOException {
        long scan = fileDataStorage.scan((position, data) -> true, (position, data) -> true);

        assertEquals(0, scan);

        for (int i = 0; i < 1_000; i++) {
            ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
            buffer.putInt(i);

            fileDataStorage.put(buffer.array(), buffer.array());
        }

        scan = fileDataStorage.scan((position, data) -> true, (position, data) -> true);

        assertEquals(1_000, scan);


        for (int i = 0; i < 1_000; i++) {
            ByteBuffer buffer = ByteBuffer.allocate(Integer.BYTES);
            buffer.putInt(i);

            fileDataStorage.put(buffer.array(), buffer.array());
        }

        scan = fileDataStorage.scan((position, data) -> true, (position, data) -> true);

        assertEquals(1_000, scan);
    }
}
