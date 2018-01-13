package com.fnklabs.dds.table;

import com.fnklabs.dds.storage.TableStorage;

public interface TableEngineFactory<O extends TableEngineOptions> {
    TableEngine get(O tableEngineOptions, TableStorage tableStorage, TableDefinition tableDefinition);
}
