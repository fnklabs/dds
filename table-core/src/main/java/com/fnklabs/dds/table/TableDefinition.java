package com.fnklabs.dds.table;

import java.util.List;

public interface TableDefinition {
    String name();

    List<ColumnDefinition> columns();
}
