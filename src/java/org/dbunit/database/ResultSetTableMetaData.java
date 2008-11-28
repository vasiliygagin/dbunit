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

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

import org.dbunit.dataset.AbstractTableMetaData;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.DefaultTableMetaData;
import org.dbunit.dataset.datatype.DataType;
import org.dbunit.dataset.datatype.IDataTypeFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link ResultSet} based {@link org.dbunit.dataset.ITableMetaData} implementation
 * @author gommma
 * @version $Revision$
 * @since 2.3.0
 */
public class ResultSetTableMetaData extends AbstractTableMetaData 
{
    /**
     * Logger for this class
     */
    private static final Logger logger = LoggerFactory.getLogger(DatabaseTableMetaData.class);

    /**
     * The actual table metadata
     */
    private DefaultTableMetaData wrappedTableMetaData;
	

	/**
	 * @param tableName The name of the database table
	 * @param resultSet The JDBC result set that is used to retrieve the columns
	 * @param connection The connection which is needed to retrieve some configuration values
	 * @throws DataSetException
	 * @throws SQLException
	 */
	public ResultSetTableMetaData(String tableName,
            ResultSet resultSet, IDatabaseConnection connection) throws DataSetException, SQLException {
		super();
		this.wrappedTableMetaData = createMetaData(tableName, resultSet, connection);
		
	}

	/**
	 * @param tableName The name of the database table
	 * @param resultSet The JDBC result set that is used to retrieve the columns
	 * @param dataTypeFactory
	 * @throws DataSetException
	 * @throws SQLException
	 */
	public ResultSetTableMetaData(String tableName,
            ResultSet resultSet, IDataTypeFactory dataTypeFactory) throws DataSetException, SQLException {
		super();
		this.wrappedTableMetaData = createMetaData(tableName, resultSet, dataTypeFactory);
	}

    private DefaultTableMetaData createMetaData(String tableName,
            ResultSet resultSet, IDatabaseConnection connection)
            throws SQLException, DataSetException
    {
    	if (logger.isDebugEnabled())
    		logger.debug("createMetaData(tableName={}, resultSet={}, connection={}) - start",
    				new Object[] { tableName, resultSet, connection });

        IDataTypeFactory typeFactory = super.getDataTypeFactory(connection);
        return createMetaData(tableName, resultSet, typeFactory);
    }

    private DefaultTableMetaData createMetaData(String tableName,
            ResultSet resultSet, IDataTypeFactory dataTypeFactory)
            throws DataSetException, SQLException
    {
    	if (logger.isDebugEnabled())
    		logger.debug("createMetaData(tableName={}, resultSet={}, dataTypeFactory={}) - start",
    				new Object[]{ tableName, resultSet, dataTypeFactory });

    	// Create the columns from the result set
        ResultSetMetaData metaData = resultSet.getMetaData();
        Column[] columns = new Column[metaData.getColumnCount()];
        for (int i = 0; i < columns.length; i++)
        {
            int columnType = metaData.getColumnType(i + 1);
            String columnTypeName = metaData.getColumnTypeName(i + 1);
            String columnName = metaData.getColumnName(i + 1);
            
            DataType dataType = dataTypeFactory.createDataType(
	                    columnType, columnTypeName, tableName, columnName);
            
            columns[i] = new Column(
                    columnName,
                    dataType,
                    columnTypeName,
                    Column.nullableValue(metaData.isNullable(i + 1)));
        }

        return new DefaultTableMetaData(tableName, columns);
    }

    
	public Column[] getColumns() throws DataSetException {
		return this.wrappedTableMetaData.getColumns();
	}

	public Column[] getPrimaryKeys() throws DataSetException {
		return this.wrappedTableMetaData.getPrimaryKeys();
	}

	public String getTableName() {
		return this.wrappedTableMetaData.getTableName();
	}

	public String toString()
	{
		StringBuffer sb = new StringBuffer();
		sb.append(getClass().getName()).append("[");
		sb.append("wrappedTableMetaData=").append(this.wrappedTableMetaData);
		sb.append("]");
		return sb.toString();
	}
}
