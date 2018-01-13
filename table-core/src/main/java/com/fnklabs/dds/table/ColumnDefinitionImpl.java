package com.fnklabs.dds.table;

import com.fnklabs.dds.table.codec.CodeсFactory;
import com.google.common.base.MoreObjects;
import lombok.EqualsAndHashCode;

@EqualsAndHashCode(of = {"name"})
public class ColumnDefinitionImpl<T> implements ColumnDefinition {
    private final String name;
    private final DataType type;
    private final boolean isPrimary;

    private final int size;

    public ColumnDefinitionImpl(String name, DataType type, boolean isPrimary) {
        this.name = name;
        this.type = type;
        this.isPrimary = isPrimary;
        this.size = CodeсFactory.codec(type).size();
    }

    protected ColumnDefinitionImpl(String name, DataType type, boolean isPrimary, int size) {
        this.name = name;
        this.type = type;
        this.isPrimary = isPrimary;
        this.size = size;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public DataType type() {
        return type;
    }

    @Override
    public int compareTo(ColumnDefinition o) {
        return name.compareTo(o.name());
    }

    @Override
    public boolean isPrimary() {
        return isPrimary;
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                          .add("name", name())
                          .add("type", type())
                          .add("isPrimary", isPrimary())
                          .toString();
    }


}
