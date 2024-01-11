/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.dataset.filter;

import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.ITableIterator;
import org.dbunit.dataset.ITableMetaData;

class FilteredTableIterator implements ITableIterator {

    private final ITableIterator _iterator;
    private final ITableFilterSimple filter;

    public FilteredTableIterator(ITableIterator iterator, ITableFilterSimple filter) {
        _iterator = iterator;
        this.filter = filter;
    }

    ////////////////////////////////////////////////////////////////////////////
    // ITableIterator interface

    @Override
    public boolean next() throws DataSetException {
        while (_iterator.next()) {
            if (filter.accept(_iterator.getTableMetaData().getTableName())) {
                return true;
            }
        }
        return false;
    }

    @Override
    public ITableMetaData getTableMetaData() throws DataSetException {
        return _iterator.getTableMetaData();
    }

    @Override
    public ITable getTable() throws DataSetException {
        return _iterator.getTable();
    }
}