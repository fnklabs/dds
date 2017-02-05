package com.fnklabs.dds.network;

import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

public class NetworkConnectorTest {
    private NetworkConnector connector = new NetworkConnector(null,null) {
    };

    public NetworkConnectorTest() throws IOException {
    }

    @Test
    public void readMessagesFromBuffer() throws Exception {
        ByteBuffer msgBuffer = ByteBuffer.allocate(Message.HEADER_SIZE * 4);

        for (int i = 0; i < 4; i++) {
            ByteBuffer msg = Message.pack(new Message(StatusCode.OK, ApiVersion.CURRENT, null));
            msgBuffer.put(msg);
        }

        AtomicInteger msgCount = new AtomicInteger();

        connector.readMessagesFromBuffer(msgBuffer, message -> {
            msgCount.getAndIncrement();
        });

        assertEquals(4, msgCount.intValue());
    }

}