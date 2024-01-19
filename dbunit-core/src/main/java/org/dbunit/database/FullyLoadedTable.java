/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.database;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.dbunit.database.metadata.ColumnMetadata;
import org.dbunit.database.metadata.TableMetadata;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITableMetaData;
import org.dbunit.dataset.ResultSetMetaData;
import org.dbunit.dataset.RowOutOfBoundsException;
import org.dbunit.dataset.datatype.DataType;
import org.dbunit.dataset.datatype.DataTypeException;
import org.dbunit.dataset.datatype.IDataTypeFactory;
import org.dbunit.dataset.datatype.TypeCastException;

public class FullyLoadedTable implements IResultSetTable {

    private final ResultSetMetaData metaData;
    private final List<Object[]> rowsData;

    public FullyLoadedTable(String tableName, String selectStatement, IDatabaseConnection connection,
            TableMetadata tableMetadata) throws DataSetException {
        try {
            int fetchSize = connection.getDatabaseConfig().getFetchSize();
            Connection jdbcConnection = connection.getConnection();
            try (Statement st = jdbcConnection.createStatement();) {
                st.setFetchSize(fetchSize);

                try (ResultSet rs = st.executeQuery(selectStatement);) {
                    metaData = buildMetaData(tableName, rs, connection, tableMetadata);
                    rowsData = readRows(rs);
                }
            }
        } catch (SQLException exc) {
            throw new DataSetException(exc);
        }
    }

    public ResultSetMetaData buildMetaData(String tableName, ResultSet resultSet, IDatabaseConnection connection,
            TableMetadata tableMetadata) throws DataSetException, SQLException {
        IDataTypeFactory typeFactory = connection.getDatabaseConfig().getDataTypeFactory();
        List<Column> tableColumns = buildTableColumns(tableMetadata, typeFactory);
        Column[] columnsArray = buildResultSetColumns(resultSet, tableColumns);
        return new ResultSetMetaData(tableName, columnsArray);
    }

    private Column[] buildResultSetColumns(ResultSet resultSet, List<Column> tableColumns) throws SQLException {
        Map<String, Column> tableColumnsByName = new HashMap<>();
        for (Column column : tableColumns) {
            tableColumnsByName.put(column.getColumnName(), column);
        }

        java.sql.ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Column[] columnsArray = new Column[resultSetMetaData.getColumnCount()];
        for (int i = 0; i < columnsArray.length; i++) {
            String columnName = resultSetMetaData.getColumnName(i + 1);
            columnsArray[i] = tableColumnsByName.get(columnName);
        }
        return columnsArray;
    }

    private List<Column> buildTableColumns(TableMetadata tableMetadata, IDataTypeFactory dataTypeFactory)
            throws DataTypeException {
        List<Column> tableColumns = new ArrayList<>();

        // Scroll resultset forward - must have one result which exactly matches the
        // required parameters
        for (ColumnMetadata columnMetadata : tableMetadata.getColumns()) {
            String tableName1 = tableMetadata.tableName;
            String columnName = columnMetadata.columnName;
            int sqlType = columnMetadata.sqlType;
            String sqlTypeName = columnMetadata.sqlTypeName;

            DataType dataType = dataTypeFactory.createDataType(sqlType, sqlTypeName, tableName1, columnName);
            Column column = new Column(columnName, dataType, sqlTypeName, Column.NULLABLE_UNKNOWN, null, null,
                    Column.AutoIncrement.UNKNOWN);
            tableColumns.add(column);
        }
        return tableColumns;
    }

    private List<Object[]> readRows(ResultSet _resultSet) throws SQLException, TypeCastException {
        List<Object[]> _rowList2 = new ArrayList<>();
        Column[] columns = metaData.getColumns();
        while (_resultSet.next()) {
            Object[] rowValues = new Object[columns.length];
            for (int j = 0; j < columns.length; j++) {
                rowValues[j] = columns[j].getDataType().getSqlValue(j + 1, _resultSet);
            }
            _rowList2.add(rowValues);
        }
        return _rowList2;
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

        Object[] rowValues = rowsData.get(row);
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
