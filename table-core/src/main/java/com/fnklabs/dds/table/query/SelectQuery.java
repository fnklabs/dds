package com.fnklabs.dds.table.query;

import java.util.List;

public class SelectQuery implements Query {
    private final List<Selection> selections;



    public SelectQuery(List<Selection> selections) {this.selections = selections;}
}
