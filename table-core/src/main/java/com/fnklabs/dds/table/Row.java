package com.fnklabs.dds.table;

public interface Row {
    Boolean getBoolean(String column);

    Byte getByte(String column);

    Double getDouble(String column);

    Float getFloat(String column);

    Integer getInt(String column);

    Long getLong(String column);

    String getString(String column);
}
