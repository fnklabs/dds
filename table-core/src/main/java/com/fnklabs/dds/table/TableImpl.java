package com.fnklabs.dds.table;

import com.fnklabs.dds.storage.TableStorage;
import com.fnklabs.dds.table.query.Query;

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
        return tableDefinition.name();
    }

    @Override
    public TableDefinition definition() {
        return tableDefinition;
    }

    @Override
    public ResultSet query(Query query) {
        return tableEngine.query(query);
    }


}
