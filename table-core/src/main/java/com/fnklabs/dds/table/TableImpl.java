package com.fnklabs.dds.table;

import com.fnklabs.dds.storage.TableStorage;

public final class TableImpl implements Table {
    private final TableDefinition tableDefinition;

    private final TableEngine tableEngine;

    private final TableStorage tableStorage;

    public TableImpl(TableDefinition tableDefinition, TableEngine tableEngine, TableStorage tableStorage) {
        this.tableDefinition = tableDefinition;
        this.tableEngine = tableEngine;
        this.tableStorage = tableStorage;
    }


    @Override
    public String name() {
        return tableDefinition.getName();
    }

    @Override
    public TableDefinition definition() {
        return tableDefinition;
    }

    @Override
    public ResultSet query(Insert insertQuery) {
        return tableEngine.query(insertQuery);
    }

    @Override
    public ResultSet query(Select selectQuery) {
        return tableEngine.query(selectQuery);
    }
}
