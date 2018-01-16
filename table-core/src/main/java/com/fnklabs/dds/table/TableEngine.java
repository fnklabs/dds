package com.fnklabs.dds.table;

public interface TableEngine {
    /**
     * Execute insert query
     *
     * @param insert Insert query instance
     *
     * @return ResultSet with insert result in {@link ResultSet#getWasApplied()}
     */
    ResultSet query(Insert insert);

    /**
     * Execute select query
     *
     * @param select Select query instance
     *
     * @return ResultSet with select result in {@link ResultSet#getResultList()} ()}
     */
    ResultSet query(Select select);
}
