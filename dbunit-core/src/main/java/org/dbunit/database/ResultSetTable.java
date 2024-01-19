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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.dbunit.DbUnitException;
import org.dbunit.database.metadata.ColumnMetadata;
import org.dbunit.database.metadata.TableMetadata;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ResultSetMetaData;
import org.dbunit.dataset.datatype.DataTypeException;
import org.dbunit.dataset.datatype.IDataTypeFactory;
import org.dbunit.dataset.datatype.TypeCastException;

public class ResultSetTable implements AutoCloseable, Iterator<ResultSetRow> {

    private final ResultSetMetaData metaData;
    private final DatabaseColumn[] columns;
    private final Statement st;
    private final ResultSet rs;
    private boolean hasNext;

    public ResultSetTable(String tableName, String selectStatement, IDatabaseConnection connection,
            TableMetadata tableMetadata) throws DataSetException {
        try {
            int fetchSize = connection.getDatabaseConfig().getFetchSize();
            Connection jdbcConnection = connection.getConnection();

            st = jdbcConnection.createStatement();
            st.setFetchSize(fetchSize);
            rs = st.executeQuery(selectStatement);
            columns = buildColumns(connection, tableMetadata, rs);
            metaData = new ResultSetMetaData(tableName, columns);

            hasNext = rs.next();
        } catch (SQLException exc) {
            throw new DataSetException(exc);
        }
    }

    private DatabaseColumn[] buildColumns(IDatabaseConnection connection, TableMetadata tableMetadata,
            ResultSet resultSet) throws DataTypeException, SQLException {
        IDataTypeFactory typeFactory = connection.getDatabaseConfig().getDataTypeFactory();
        List<DatabaseColumn> tableColumns = buildTableColumns(tableMetadata, typeFactory);
        return buildResultSetColumns(resultSet, tableColumns);
    }

    private DatabaseColumn[] buildResultSetColumns(ResultSet resultSet, List<DatabaseColumn> tableColumns)
            throws SQLException {
        Map<String, DatabaseColumn> tableColumnsByName = new HashMap<>();
        for (DatabaseColumn column : tableColumns) {
            tableColumnsByName.put(column.getColumnName(), column);
        }

        java.sql.ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        DatabaseColumn[] columnsArray = new DatabaseColumn[resultSetMetaData.getColumnCount()];
        for (int i = 0; i < columnsArray.length; i++) {
            String columnName = resultSetMetaData.getColumnName(i + 1);
            columnsArray[i] = tableColumnsByName.get(columnName);
        }
        return columnsArray;
    }

    private List<DatabaseColumn> buildTableColumns(TableMetadata tableMetadata, IDataTypeFactory dataTypeFactory)
            throws DataTypeException {
        List<DatabaseColumn> tableColumns = new ArrayList<>();

        for (ColumnMetadata columnMetadata : tableMetadata.getColumns()) {
            DatabaseColumn column = new DatabaseColumn(tableMetadata, columnMetadata, dataTypeFactory);
            tableColumns.add(column);
        }
        return tableColumns;
    }

    @Override
    public boolean hasNext() {
        return hasNext;
    }

    @Override
    public ResultSetRow next() {
        if (!hasNext) {
            throw new NoSuchElementException();
        }

        Object[] rowValues = new Object[columns.length];
        for (int i = 0; i < columns.length; i++) {
            DatabaseColumn column = columns[i];
            try {
                rowValues[i] = column.getDataType().getSqlValue(i + 1, rs);
            } catch (TypeCastException | SQLException exc) {
                throw new DbUnitException("Unable to read value of column [" + column.getColumnName() + "] from table ["
                        + column.tableMetadata.tableName + "]", exc);
            }
        }

        try {
            hasNext = rs.next();
        } catch (SQLException exc) {
            throw new DbUnitException("", exc);
        }

        return new ResultSetRow(columns, rowValues);
    }

    ////////////////////////////////////////////////////////////////////////////
    // IResultSetTable interface

    @Override
    public void close() throws SQLException {
        rs.close();
        st.close();
    }

    public ResultSetMetaData getMetaData() {
        return metaData;
    }
}
