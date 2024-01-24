package org.dbunit.dataset;

import org.dbunit.dataset.filter.ITableFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FilteredDataSet implements IDataSet {

    /**
     * Logger for this class
     */
    private static final Logger logger = LoggerFactory.getLogger(FilteredDataSet.class);

    private final IDataSet _dataSet;
    private final ITableFilter _filter;

    private OrderedTableNameMap<ITable> _orderedTableNameMap;

    /**
     * Whether or not table names of this dataset are case sensitive. By default
     * case-sensitivity is set to false for datasets
     */
    private boolean _caseSensitiveTableNames = false;

    /**
     * Creates a FilteredDataSet that decorates the specified dataset and exposes
     * only the tables allowed by the specified filter.
     *
     * @param dataSet the filtered dataset
     * @param filter  the filtering strategy
     */
    public FilteredDataSet(ITableFilter filter, IDataSet dataSet) {
        _caseSensitiveTableNames = dataSet.isCaseSensitiveTableNames();
        _dataSet = dataSet;
        _filter = filter;
    }

    ////////////////////////////////////////////////////////////////////////////
    // AbstractDataSet class

    private ITableIterator createIterator(boolean reversed) throws DataSetException {
        return _filter.iterator(_dataSet, reversed);
    }

    ////////////////////////////////////////////////////////////////////////////
    // IDataSet interface

    @Override
    public String[] getTableNames() throws DataSetException {
        return _filter.getTableNames(_dataSet);
    }

    @Override
    public ITableMetaData getTableMetaData(String tableName) throws DataSetException {
        if (!_filter.accept(tableName)) {
            throw new NoSuchTableException(tableName);
        }

        return _dataSet.getTableMetaData(tableName);
    }

    @Override
    public ITable getTable(String tableName) throws DataSetException {
        logger.debug("getTable(tableName={}) - start", tableName);

        if (!_filter.accept(tableName)) {
            throw new NoSuchTableException(tableName);
        }

        return _dataSet.getTable(tableName);
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
     * Initializes the tables of this dataset
     *
     * @throws DataSetException
     * @since 2.4
     */
    private void initialize() throws DataSetException {
        if (_orderedTableNameMap != null) {
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

    @Override
    public ITable[] getTables() throws DataSetException {
        initialize();

        return this._orderedTableNameMap.orderedValues().toArray(new ITable[0]);
    }

    @Override
    public ITableIterator iterator() throws DataSetException {
        return createIterator(false);
    }

    @Override
    public ITableIterator reverseIterator() throws DataSetException {
        return createIterator(true);
    }
}
