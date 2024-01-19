/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.database;

import java.util.ArrayList;
import java.util.List;

import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITableMetaData;
import org.dbunit.dataset.ResultSetMetaData;
import org.dbunit.dataset.RowOutOfBoundsException;

public class FullyLoadedTable implements IResultSetTable {

    private final ResultSetMetaData metaData;
    private final List<ResultSetRow> rowsData = new ArrayList<>();

    public FullyLoadedTable(ResultSetTable resultSetTable) {
        metaData = resultSetTable.getMetaData();
        while (resultSetTable.hasNext()) {
            rowsData.add(resultSetTable.next());
        }
    }

    ////////////////////////////////////////////////////////////////////////////
    // IResultSetTable interface

    @Override
    public void close() throws DataSetException {
        // nothing to do, resultset already been closed
    }

    @Override
    public ITableMetaData getTableMetaData() {
        return metaData;
    }

    @Override
    public int getRowCount() {
        return rowsData.size();
    }

    @Override
    public Object getValue(int row, String column) throws DataSetException {
        assertValidRowIndex(row);

        Object[] rowValues = rowsData.get(row).getRowValues();
        return rowValues[getColumnIndex(column)];
    }

    private void assertValidRowIndex(int row) throws DataSetException {
        assertValidRowIndex(row, getRowCount());
    }

    private void assertValidRowIndex(int row, int rowCount) throws DataSetException {
        if (row < 0) {
            throw new RowOutOfBoundsException(row + " < 0");
        }

        if (row >= rowCount) {
            throw new RowOutOfBoundsException(row + " >= " + rowCount);
        }
    }

    private int getColumnIndex(String columnName) throws DataSetException {
        ITableMetaData metaData = getTableMetaData();
        return metaData.getColumnIndex(columnName);
    }
}
