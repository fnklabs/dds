package com.fnklabs.dds.network.connector;

import java.nio.ByteBuffer;

public class Fragmentation {


    public static final int MESSAGE_SIZE = Integer.BYTES;

    /**
     * @param data
     *
     * @return
     */
    public static int getMessageLength(ByteBuffer data) {
        return data.getInt(0);
    }

}
