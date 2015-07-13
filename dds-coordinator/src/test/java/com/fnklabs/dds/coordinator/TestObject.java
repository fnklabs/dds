package com.fnklabs.dds.coordinator;

import com.google.common.base.Objects;

import java.io.Serializable;
import java.util.UUID;

public class TestObject implements Serializable {


    private static final long serialVersionUID = -7850410791669817849L;

    private UUID field1 = UUID.randomUUID();
    private UUID field2 = UUID.randomUUID();

    public UUID getField1() {
        return field1;
    }

    public void setField1(UUID field1) {
        this.field1 = field1;
    }


    @Override
    public String toString() {
        return Objects
                .toStringHelper(getClass())
                .add("field1", field1)
                .add("field2", field2)
                .toString();
    }

}
