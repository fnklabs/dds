package com.fnklabs.dds.table;

import com.fnklabs.dds.table.query.Query;

public interface Table {
    String name();

    TableDefinition definition();

    ResultSet query(Query query);
}
