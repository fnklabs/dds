package com.fnklabs.dds.storage.columnar;

import com.fnklabs.dds.storage.TableStorage;
import com.fnklabs.dds.table.TableDefinition;
import com.fnklabs.dds.table.TableEngine;
import com.fnklabs.dds.table.TableEngineFactory;

public class ColumnarEngineFactory implements TableEngineFactory<ColumnarOptions> {
    @Override
    public TableEngine get(ColumnarOptions tableEngineOptions, TableStorage tableStorage, TableDefinition tableDefinition) {
        return new ColumnarTableEngine(tableDefinition, tableStorage, tableEngineOptions.getChunks(), tableEngineOptions.getChunkSize());
    }
}
