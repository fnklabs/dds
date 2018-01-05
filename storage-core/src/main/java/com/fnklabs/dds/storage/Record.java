package com.fnklabs.dds.storage;

import com.fnklabs.dds.storage.column.Column;
import org.apache.commons.lang3.StringUtils;

import java.nio.ByteBuffer;
import java.util.Map;

public class Record {
    private final Map<Column, Object> values;

    public Record(Map<Column, Object> values) {
        this.values = values;
    }

    /**
     * Read column value and write it to buffer
     *
     * @param column
     * @param buffer
     */
    public void get(Column column, ByteBuffer buffer) {
        Object value = values.get(column);

        column.write(value, buffer);
    }

    public Column getPrimary() {
        return values.keySet()
                     .stream()
                     .filter(Column::isPrimary)
                     .findFirst()
                     .orElse(null);
    }

    public <T> T get(Column<T> column) {
        return (T) values.get(column);
    }

    public <T> T get(String column) {
        return (T) values.keySet()
                         .stream()
                         .filter(c -> StringUtils.equals(column, c.name()))
                         .findFirst()
                         .map(c -> values.get(c))
                         .orElse(null);
    }
}
