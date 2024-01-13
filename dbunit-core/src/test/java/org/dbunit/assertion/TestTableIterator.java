/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.assertion;

import java.util.Arrays;
import java.util.Iterator;

import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.ITableIterator;
import org.dbunit.dataset.ITableMetaData;

/**
 *
 */
public class TestTableIterator implements ITableIterator {

    private Iterator<TestTable> iterator;
    private TestTable currentTable;

    public TestTableIterator(TestTable[] testTables) {
        iterator = Arrays.asList(testTables).iterator();
    }

    @Override
    public boolean next() throws DataSetException {
        if (iterator.hasNext()) {
            currentTable = iterator.next();
            return true;
        }
        return false;
    }

    @Override
    public ITableMetaData getTableMetaData() throws DataSetException {
        return currentTable.getTableMetaData();
    }

    @Override
    public ITable getTable() throws DataSetException {
        return currentTable;
    }
}
