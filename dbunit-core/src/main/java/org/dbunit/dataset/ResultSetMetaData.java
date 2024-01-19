/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.dataset;

import java.util.HashMap;
import java.util.Map;

public class ResultSetMetaData implements ITableMetaData {

    private final String _tableName;
    private final Column[] _columns;
    private final Column[] _primaryKeys;
    private Map<String, Integer> _columnsToIndexes;

    public ResultSetMetaData(String tableName, Column[] columns) {
        _tableName = tableName;
        _columns = columns;
        _primaryKeys = new Column[0];
    }

    @Override
    public String getTableName() {
        return _tableName;
    }

    @Override
    public Column[] getColumns() {
        return _columns;
    }

    @Override
    public Column[] getPrimaryKeys() {
        return _primaryKeys;
    }

    /**
     * Provides the index of the column with the given name within this table. Uses
     * method {@link ITableMetaData#getColumns()} to retrieve all available columns.
     *
     * @throws DataSetException
     * @see org.dbunit.dataset.ITableMetaData#getColumnIndex(java.lang.String)
     */
    @Override
    public int getColumnIndex(String columnName) throws DataSetException {

        if (this._columnsToIndexes == null) {
            // lazily create the map
            this._columnsToIndexes = createColumnIndexesMap(this._columns);
        }

        String columnNameUpperCase = columnName.toUpperCase();
        Integer colIndex = this._columnsToIndexes.get(columnNameUpperCase);
        if (colIndex != null) {
            return colIndex.intValue();
        }
        throw new NoSuchColumnException(this.getTableName(), columnNameUpperCase,
                " (Non-uppercase input column: " + columnName + ") in ColumnNameToIndexes cache map. "
                        + "Note that the map's column names are NOT case sensitive.");
    }

    /**
     * @param columns The columns to be put into the hash table
     * @return A map having the key value pair [columnName, columnIndexInInputArray]
     */
    private Map<String, Integer> createColumnIndexesMap(Column[] columns) {
        Map<String, Integer> colsToIndexes = new HashMap<>(columns.length);
        for (int i = 0; i < columns.length; i++) {
            colsToIndexes.put(columns[i].getColumnName().toUpperCase(), new Integer(i));
        }
        return colsToIndexes;
    }
}
