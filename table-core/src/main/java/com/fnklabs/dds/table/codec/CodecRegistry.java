package com.fnklabs.dds.table.codec;

import com.fnklabs.dds.table.DataType;
import com.google.common.base.Verify;

import java.util.HashMap;
import java.util.Map;

public final class CodecRegistry {
    private final static Map<DataType, DataTypeCodec> codecs = new HashMap<>();

    static {
        codecs.put(DataType.BOOLEAN, new BooleanCode());
        codecs.put(DataType.BYTE, new ByteCodec());
        codecs.put(DataType.INT, new IntCodec());
        codecs.put(DataType.LONG, new LongCodec());
        codecs.put(DataType.FLOAT, new FloatCodec());
        codecs.put(DataType.DOUBLE, new DoubleCodec());
        codecs.put(DataType.STRING, new StringCodec());
    }

    private CodecRegistry() {}

    public static DataTypeCodec get(DataType dataType) {
        DataTypeCodec codec = codecs.get(dataType);

        Verify.verifyNotNull(codec, "codec not found");

        return codec;
    }
}
