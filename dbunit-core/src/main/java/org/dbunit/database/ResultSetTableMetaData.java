/*
 *
 * The DbUnit Database Testing Framework
 * Copyright (C)2002-2008, DbUnit.org
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
package org.dbunit.database;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.dbunit.dataset.AbstractTableMetaData;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.DefaultTableMetaData;
import org.dbunit.dataset.datatype.DataType;
import org.dbunit.dataset.datatype.DataTypeException;
import org.dbunit.dataset.datatype.IDataTypeFactory;
import org.dbunit.util.SQLHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link ResultSet} based {@link org.dbunit.dataset.ITableMetaData}
 * implementation.
 * <p>
 * The lookup for the information needed to create the {@link Column} objects is
 * retrieved in two phases:
 * <ol>
 * <li>Try to find the information from the given {@link ResultSet} via a
 * {@link DatabaseMetaData} object. Therefore the {@link ResultSetMetaData} is
 * used to get the catalog/schema/table/column names which in turn are used to
 * get column information via
 * {@link DatabaseMetaData#getColumns(String, String, String, String)}. The
 * reason for this is that the {@link DatabaseMetaData} is more precise and
 * contains more information about columns than the {@link ResultSetMetaData}
 * does. Another reason is that some JDBC drivers (currently known from MYSQL
 * driver) provide an inconsistent implementation of those two MetaData objects
 * and the {@link DatabaseMetaData} is hence considered to be the master by
 * dbunit.</li>
 * <li>Since some JDBC drivers (one of them being Oracle) cannot (or just do
 * not) provide the catalog/schema/table/column values on a
 * {@link ResultSetMetaData} instance the second step will create the dbunit
 * {@link Column} using the {@link ResultSetMetaData} methods directly (for
 * example {@link ResultSetMetaData#getColumnType(int)}. (This is also the way
 * dbunit worked until the 2.4 release)</li>
 * </ol>
 * </p>
 *
 * @author gommma (gommma AT users.sourceforge.net)
 * @author Last changed by: $Author$
 * @version $Revision$ $Date$
 * @since 2.3.0
 */
public class ResultSetTableMetaData extends AbstractTableMetaData {

    /**
     * Logger for this class
     */
    private static final Logger logger = LoggerFactory.getLogger(DatabaseTableMetaData.class);

    /**
     * The actual table metadata
     */
    private DefaultTableMetaData wrappedTableMetaData;
    private boolean _caseSensitiveMetaData;

    /**
     * @param tableName             The name of the database table
     * @param resultSet             The JDBC result set that is used to retrieve the
     *                              columns
     * @param connection            The connection which is needed to retrieve some
     *                              configuration values
     * @param caseSensitiveMetaData Whether or not the metadata is case sensitive
     * @throws DataSetException
     * @throws SQLException
     */
    public ResultSetTableMetaData(String tableName, ResultSet resultSet, IDatabaseConnection connection,
            boolean caseSensitiveMetaData) throws DataSetException, SQLException {
        _caseSensitiveMetaData = caseSensitiveMetaData;
        this.wrappedTableMetaData = createMetaData(tableName, resultSet, connection);

    }

    private DefaultTableMetaData createMetaData(String tableName, ResultSet resultSet, IDatabaseConnection connection)
            throws SQLException, DataSetException {
        IMetadataHandler columnFactory = connection.getDatabaseConfig().getMetadataHandler();
        IDataTypeFactory typeFactory = super.getDataTypeFactory(connection);
        return createMetaData(tableName, resultSet, typeFactory, columnFactory);
    }

    private DefaultTableMetaData createMetaData(String tableName, ResultSet resultSet, IDataTypeFactory dataTypeFactory,
            IMetadataHandler columnFactory) throws DataSetException, SQLException {
        Connection connection = resultSet.getStatement().getConnection();

        DatabaseMetaData databaseMetaData = connection.getMetaData();

        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        Column[] columns = new Column[resultSetMetaData.getColumnCount()];
        for (int i = 0; i < columns.length; i++) {
            int rsIndex = i + 1;
            Column column = buildColumn(tableName, dataTypeFactory, columnFactory, databaseMetaData, resultSetMetaData,
                    rsIndex);
            columns[i] = column;
        }

        return new DefaultTableMetaData(tableName, columns);
    }

    private Column buildColumn(String tableName, IDataTypeFactory dataTypeFactory, IMetadataHandler columnFactory,
            DatabaseMetaData databaseMetaData, ResultSetMetaData resultSetMetaData, int rsIndex)
            throws SQLException, DataTypeException {

        // Due to a bug in the DB2 JDBC driver we have to trim the names
        String catalogName = trim(resultSetMetaData.getCatalogName(rsIndex));
        String schemaName = trim(resultSetMetaData.getSchemaName(rsIndex));
        String tableName1 = trim(resultSetMetaData.getTableName(rsIndex));
        String columnName = trim(resultSetMetaData.getColumnLabel(rsIndex));

        // Check if at least one of catalog/schema/table attributes is
        // not applicable (i.e. "" is returned). If so do not try
        // to get the column metadata from the DatabaseMetaData object.
        // This is the case for all oracle JDBC drivers
        if (catalogName != null && catalogName.equals("")) {
            catalogName = null;
        }

        Column column = null;
        if (schemaName == null || !schemaName.equals("")) {
            if (tableName1 == null || !tableName1.equals("")) {

                try ( //
                        ResultSet columnsResultSet = databaseMetaData.getColumns(columnFactory.toCatalog(schemaName),
                                columnFactory.toSchema(schemaName), tableName1, "%"); //
                ) {
                    // Scroll resultset forward - must have one result which exactly matches the
                    // required parameters
                    boolean found = false;
                    while (columnsResultSet.next()) {
                        boolean match = columnFactory.matches(columnsResultSet, catalogName, schemaName, tableName1,
                                columnName, _caseSensitiveMetaData);
                        if (match) {
                            // All right. Return immediately because the resultSet is positioned on the
                            // correct row
                            found = true;
                            break;
                        }
                    }

                    if (!found) {
                        logger.warn(
                                "Cannot find column from ResultSetMetaData info via DatabaseMetaData. Returning null."
                                        + " Even if this is expected to never happen it probably happened due to a JDBC driver bug."
                                        + " To get around this you may want to configure a user defined "
                                        + IMetadataHandler.class);
                        logger.warn("Did not find column '" + columnName + "' for <schema.table> '" + schemaName + "."
                                + tableName1 + "' in catalog '" + catalogName
                                + "' because names do not exactly match.");
                    } else {
                        column = SQLHelper.createColumn(columnsResultSet, dataTypeFactory, true);
                    }
                }
            }
        }
        if (column == null) {
            int columnType = resultSetMetaData.getColumnType(rsIndex);
            String columnTypeName = resultSetMetaData.getColumnTypeName(rsIndex);
            String columnName1 = resultSetMetaData.getColumnLabel(rsIndex);
            int isNullable = resultSetMetaData.isNullable(rsIndex);

            DataType dataType = dataTypeFactory.createDataType(columnType, columnTypeName, tableName, columnName1);
            column = new Column(columnName1, dataType, columnTypeName, Column.nullableValue(isNullable));
        }
        return column;
    }

    /**
     * Trims the given string in a null-safe way
     *
     * @param value
     * @return
     * @since 2.4.6
     */
    private String trim(String value) {
        return (value == null ? null : value.trim());
    }

    @Override
    public Column[] getColumns() throws DataSetException {
        return this.wrappedTableMetaData.getColumns();
    }

    @Override
    public Column[] getPrimaryKeys() throws DataSetException {
        return this.wrappedTableMetaData.getPrimaryKeys();
    }

    @Override
    public String getTableName() {
        return this.wrappedTableMetaData.getTableName();
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append(getClass().getName()).append("[");
        sb.append("wrappedTableMetaData=").append(this.wrappedTableMetaData);
        sb.append("]");
        return sb.toString();
    }
}
