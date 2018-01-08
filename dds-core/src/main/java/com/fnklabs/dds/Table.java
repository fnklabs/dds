package com.fnklabs.dds;

import java.sql.ResultSet;

public interface Table {
    String name();

    long count();

    long size();

    ResultSet query(Query query);
}
