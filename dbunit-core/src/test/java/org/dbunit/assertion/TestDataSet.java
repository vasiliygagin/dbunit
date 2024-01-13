/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.assertion;

import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.ITableIterator;
import org.dbunit.dataset.ITableMetaData;

/**
 *
 */
public class TestDataSet implements IDataSet {

    private TestTable[] testTables;

    public TestDataSet(TestTable... testTables) {
        this.testTables = testTables;
    }

    @Override
    public String[] getTableNames() throws DataSetException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ITableMetaData getTableMetaData(String tableName) throws DataSetException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ITable getTable(String tableName) throws DataSetException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ITable[] getTables() throws DataSetException {
        throw new UnsupportedOperationException();
    }

    @Override
    public ITableIterator iterator() throws DataSetException {
        return new TestTableIterator(testTables);
    }

    @Override
    public ITableIterator reverseIterator() throws DataSetException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isCaseSensitiveTableNames() {
        throw new UnsupportedOperationException();
    }
}
