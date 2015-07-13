package com.fnklabs.dds;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.kryo.pool.KryoFactory;
import com.esotericsoftware.kryo.pool.KryoPool;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

public class SerializationUtils {


    private static final KryoFactory factory = () -> {
        // configure kryo instance, customize settings
        return new Kryo();
    };

    // Build pool with SoftReferences enabled (optional)
    private static final KryoPool pool = new KryoPool.Builder(factory).softReferences().build();


    public static <T> T read(ByteBuffer serializedObject) {
        Kryo borrow = pool.borrow();

        Input input = new Input(serializedObject.array());

        T deseriazedObject = (T) borrow.readClassAndObject(input);

        pool.release(borrow);

        return deseriazedObject;
    }

    public static <T> ByteBuffer write(T t) {
        Kryo borrow = pool.borrow();

        Output output = new Output(new ByteArrayOutputStream());
        borrow.writeClassAndObject(output, t);

        return ByteBuffer.wrap(output.getBuffer());
    }

}
