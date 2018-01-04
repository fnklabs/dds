package com.fnklabs.dds.storage.column;

import com.google.common.base.MoreObjects;

import java.util.Objects;

public abstract class AbstractColumn<T> implements Column<T> {
    private final String name;
    private final Class<T> clazz;
    private final short size;
    private final short order;
    private final boolean isPrimary;

    public AbstractColumn(String name, Class<T> clazz, short size, short order, boolean isPrimary) {
        this.name = name;
        this.clazz = clazz;
        this.size = size;
        this.order = order;
        this.isPrimary = isPrimary;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public Class<T> type() {
        return clazz;
    }

    @Override
    public short size() {
        return size;
    }

    @Override
    public short order() {
        return order;
    }

    @Override
    public boolean isPrimary() {
        return isPrimary;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                          .add("name", name)
                          .add("size", size)
                          .add("order", order)
                          .add("isPrimary", isPrimary)
                          .toString();
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(name());
    }

    @Override
    public boolean equals(Object obj) {
        if (obj.getClass().equals(getClass())) {
            AbstractColumn cast = getClass().cast(obj);

            return Objects.equals(name(), cast.name());
        }
        return false;
    }
}
