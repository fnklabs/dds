package com.fnklabs.dds.storage;

import com.fnklabs.dds.DdsVersion;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.UUID;

import static org.junit.Assert.*;

public class MetaInformationTest {
    @Test
    public void unpack() throws Exception {

    }

    @Test
    public void pack() throws Exception {
        MetaInformation metaInformation = new MetaInformation(
                DdsVersion.CURRENT,
                new UUID(0, 1),
                Integer.BYTES,
                0,
                0
        );

        ByteBuffer buffer = MetaInformation.pack(metaInformation);

        buffer.rewind();

        MetaInformation unpack = MetaInformation.unpack(buffer);

        assertEquals(metaInformation, unpack);
    }

}