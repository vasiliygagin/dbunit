/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.assertion;

import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITableMetaData;
import org.dbunit.dataset.NoSuchColumnException;
import org.dbunit.dataset.datatype.DataType;

/**
 *
 */
public class TestTableMetaData implements ITableMetaData {

    private final String tableName;
    private final Column[] columns;

    public TestTableMetaData(String tableName, String... columnNames) {
        this.tableName = tableName;
        this.columns = new Column[columnNames.length];
        for (int i = 0; i < columnNames.length; ++i) {
            columns[i] = new Column(columnNames[i], DataType.VARCHAR);
        }
    }

    @Override
    public String getTableName() {
        return tableName;
    }

    @Override
    public Column[] getColumns() throws DataSetException {
        return columns;
    }

    @Override
    public Column[] getPrimaryKeys() throws DataSetException {
        return new Column[0];
    }

    @Override
    public int getColumnIndex(String columnName) throws DataSetException {
        for (int i = 0; i < columns.length; ++i) {
            if (columns[i].getColumnName().equals(columnName)) {
                return i;
            }
        }
        throw new NoSuchColumnException(tableName, columnName);
    }

}
