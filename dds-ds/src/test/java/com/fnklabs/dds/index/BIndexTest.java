package com.fnklabs.dds.index;

import com.fnklabs.dds.index.BIndex;
import com.fnklabs.dds.index.IndexExists;
import org.junit.*;

import java.io.File;
import java.nio.ByteBuffer;

@Ignore
public class BIndexTest {
    private static final File INDEX_FILE = new File("test.idx");
    public static final int MAX_INSERTS = 100_000;

    @Before
    public void setUp() throws Exception {
        if (INDEX_FILE.exists()) {
            INDEX_FILE.delete();
            Assert.assertFalse(INDEX_FILE.exists());
        }
    }

    @After
    public void tearDown() throws Exception {
        if (INDEX_FILE.exists()) {
            INDEX_FILE.delete();
        }
    }

    @Test
    public void createIfNotExists() throws Exception {
        BIndex bIndex = BIndex.create(INDEX_FILE, Long.BYTES);

        Assert.assertTrue(INDEX_FILE.exists());
        Assert.assertNotEquals(0, INDEX_FILE.length());
    }

    @Test(expected = IndexExists.class)
    public void createIfExists() throws Exception {
        BIndex bIndex = BIndex.create(INDEX_FILE, Long.BYTES);
        bIndex.close();

        BIndex.create(INDEX_FILE, Long.BYTES);
    }

    @Test
    public void get() throws Exception {

    }

    @Test
    public void put() throws Exception {
        BIndex index = BIndex.create(INDEX_FILE, Long.BYTES);

        ByteBuffer buffer = ByteBuffer.allocate(Long.BYTES);

        for (long i = 0; i < MAX_INSERTS; i++) {
            buffer.putLong(0, i);
            boolean result = index.put(buffer.array(), i);
            Assert.assertTrue(result);

//            Optional<Long> position = index.get(key);

//            Assert.assertTrue(position.isPresent());
//            Assert.assertEquals(i, position.get().longValue());
        }

        Assert.assertEquals(MAX_INSERTS, index.length());
    }

    @Test
    public void length() throws Exception {

    }

    @Test
    public void BYTES() throws Exception {

    }

    @Test
    public void destroy() throws Exception {

    }

    @Test
    public void close() throws Exception {

    }

}