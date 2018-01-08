package com.fnklabs.dds.cluster;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.ByteBufferInput;
import com.esotericsoftware.kryo.io.ByteBufferOutput;
import com.fnklabs.dds.Cluster;

import java.nio.ByteBuffer;

public class KryoSerializer implements Serializer {
    private static final ThreadLocal<Kryo> kryos = ThreadLocal.withInitial(Kryo::new);

    @Override
    public <T> ByteBuffer write(T object) {
        Kryo kryo = kryos.get();



        try (ByteBufferOutput output = new ByteBufferOutput()) {
            kryo.writeClassAndObject(output, object);

            return output.getByteBuffer();
        }
    }

    @Override
    public <T> T read(ByteBuffer buffer, Class<T> clazz) {
        Kryo kryo = kryos.get();

        try (ByteBufferInput input = new ByteBufferInput(buffer)) {
            return kryo.readObjectOrNull(input, clazz);
        }
    }
}
