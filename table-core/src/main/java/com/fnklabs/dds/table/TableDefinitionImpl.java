package com.fnklabs.dds.table;

import lombok.EqualsAndHashCode;

import java.util.List;

@EqualsAndHashCode(of = {"name"})
public class TableDefinitionImpl implements TableDefinition {
    private final String name;
    private final List<ColumnDefinition> columnDefinitions;

    public TableDefinitionImpl(String name, List<ColumnDefinition> columnDefinitions) {
        this.name = name;
        this.columnDefinitions = columnDefinitions;
    }

    @Override
    public String name() {
        return name;
    }

    @Override
    public List<ColumnDefinition> columns() {
        return columnDefinitions;
    }
}
