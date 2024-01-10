/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.assertion;

import java.util.ArrayList;
import java.util.List;

import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.ITableMetaData;

/**
 * Column names are just numbers starting from "0", good enough for test.
 * No checks made as for column counts in the row. Be careful.
 */
public class TestTable implements ITable {

    private TestTableMetaData tableMetaData;
    private List<Object[]> rows = new ArrayList<>();

    public TestTable(String tableName, String... columnNames) {
        tableMetaData = new TestTableMetaData(tableName, columnNames);
    }

    public void addRow(Object... columnValues) {
        rows.add(columnValues);
    }

    @Override
    public ITableMetaData getTableMetaData() {
        return tableMetaData;
    }

    @Override
    public int getRowCount() {
        return rows.size();
    }

    @Override
    public Object getValue(int row, String column) throws DataSetException {
        return rows.get(row)[tableMetaData.getColumnIndex(column)];
    }
}
