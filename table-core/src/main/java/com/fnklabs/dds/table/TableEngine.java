package com.fnklabs.dds.table;

import com.fnklabs.dds.table.query.Query;

public interface TableEngine {
    ResultSet query(Query query);
}
