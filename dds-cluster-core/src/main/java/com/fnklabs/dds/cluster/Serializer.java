package com.fnklabs.dds.cluster;

import java.nio.ByteBuffer;

public interface Serializer {
    <T> ByteBuffer write(T object);

    <T> T read(ByteBuffer buffer, Class<T> clazz);
}
