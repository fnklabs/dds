package com.fnklabs.dds.table;

public interface Table {
    String name();

    TableDefinition definition();

    ResultSet query(Insert insertQuery);

    ResultSet query(Select selectQuery);
}
