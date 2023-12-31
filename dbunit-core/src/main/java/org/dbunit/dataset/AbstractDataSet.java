/*
 *
 * The DbUnit Database Testing Framework
 * Copyright (C)2002-2004, DbUnit.org
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 */

package org.dbunit.dataset;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This abstract class provides the basic implementation of the IDataSet
 * interface. Subclass are only required to implement the
 * {@link #createIterator} method.
 *
 * @author Manuel Laflamme
 * @author Last changed by: $Author$
 * @version $Revision$ $Date$
 * @since 1.0 (Feb 22, 2002)
 */
public abstract class AbstractDataSet implements IDataSet {

    protected OrderedTableNameMap<ITable> _orderedTableNameMap;

    /**
     * Whether or not table names of this dataset are case sensitive. By default
     * case-sensitivity is set to false for datasets
     */
    private boolean _caseSensitiveTableNames = false;

    /**
     * Logger for this class
     */
    private static final Logger logger = LoggerFactory.getLogger(AbstractDataSet.class);

    /**
     * Default constructor
     */
    public AbstractDataSet() {
    }

    /**
     * Constructor
     *
     * @param caseSensitiveTableNames Whether or not table names should be case
     *                                sensitive
     * @since 2.4
     */
    public AbstractDataSet(boolean caseSensitiveTableNames) {
        _caseSensitiveTableNames = caseSensitiveTableNames;
    }

    /**
     * @return <code>true</code> if the case sensitivity of table names is used in
     *         this dataset.
     * @since 2.4
     */
    @Override
    public boolean isCaseSensitiveTableNames() {
        return this._caseSensitiveTableNames;
    }

    /**
     * Creates and returns a new instance of the table names container. Implementors
     * should use this method to retrieve a map which stores table names which can
     * be linked with arbitrary objects.
     *
     * @return a new empty instance of the table names container
     * @since 2.4
     */
    protected final OrderedTableNameMap<ITable> createTableNameMap() {
        return new OrderedTableNameMap<>(isCaseSensitiveTableNames());
    }

    /**
     * Initializes the tables of this dataset
     *
     * @throws DataSetException
     * @since 2.4
     */
    protected void initialize() throws DataSetException {
        logger.debug("initialize() - start");

        if (_orderedTableNameMap != null) {
            logger.debug("The table name map has already been initialized.");
            // already initialized
            return;
        }

        // Gather all tables in the OrderedTableNameMap which also makes the duplicate
        // check
        _orderedTableNameMap = new OrderedTableNameMap<>(this.isCaseSensitiveTableNames());
        ITableIterator iterator = createIterator(false);
        while (iterator.next()) {
            ITable table = iterator.getTable();
            _orderedTableNameMap.add(table.getTableMetaData().getTableName(), table);
        }
    }

    /**
     * Creates an iterator which provides access to all tables of this dataset
     *
     * @param reversed Whether the created iterator should be a reversed one or not
     * @return The created {@link ITableIterator}
     * @throws DataSetException
     */
    protected abstract ITableIterator createIterator(boolean reversed) throws DataSetException;

    ////////////////////////////////////////////////////////////////////////////
    // IDataSet interface

    @Override
    public String[] getTableNames() throws DataSetException {
        logger.debug("getTableNames() - start");

        initialize();

        return this._orderedTableNameMap.getTableNames();
    }

    @Override
    public ITableMetaData getTableMetaData(String tableName) throws DataSetException {
        logger.debug("getTableMetaData(tableName={}) - start", tableName);

        return getTable(tableName).getTableMetaData();
    }

    @Override
    public ITable getTable(String tableName) throws DataSetException {
        initialize();

        ITable found = _orderedTableNameMap.get(tableName);
        if (found != null) {
            return found;
        } else {
            throw new NoSuchTableException(tableName);
        }
    }

    @Override
    public ITable[] getTables() throws DataSetException {
        logger.debug("getTables() - start");

        initialize();

        return this._orderedTableNameMap.orderedValues().toArray(new ITable[0]);
    }

    @Override
    public ITableIterator iterator() throws DataSetException {
        logger.debug("iterator() - start");

        return createIterator(false);
    }

    @Override
    public ITableIterator reverseIterator() throws DataSetException {
        logger.debug("reverseIterator() - start");

        return createIterator(true);
    }
}
