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
package org.dbunit.database;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.ITableMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.vasiliygagin.dbunit.jdbc.DatabaseConfig;

/**
 * @author manuel.laflamme
 * @author Last changed by: $Author$
 * @version $Revision$ $Date$
 * @since 2.0 (Jul 31, 2003)
 */
public class ForwardOnlyResultSetTableFactory implements IResultSetTableFactory {

    /**
     * Logger for this class
     */
    private static final Logger logger = LoggerFactory.getLogger(ForwardOnlyResultSetTableFactory.class);

    @Override
    public IResultSetTable createTable(String tableName, String selectStatement, IDatabaseConnection connection)
            throws SQLException, DataSetException {
        if (logger.isTraceEnabled())
            logger.trace("createTable(tableName={}, selectStatement={}, connection={}) - start", tableName,
                    selectStatement, connection);

        return new ForwardOnlyResultSetTable(tableName, selectStatement, connection);
    }

    @Override
    public IResultSetTable createTable(ITableMetaData metaData, IDatabaseConnection connection)
            throws SQLException, DataSetException {
        logger.trace("createTable(metaData={}, connection={}) - start", metaData, connection);

        return new ForwardOnlyResultSetTable(metaData, connection);
    }

    @Override
    public IResultSetTable createTable(String tableName, PreparedStatement preparedStatement,
            IDatabaseConnection connection) throws SQLException, DataSetException {
        if (logger.isTraceEnabled())
            logger.trace("createTable(tableName={}, preparedStatement={}, connection={}) - start", tableName,
                    preparedStatement, connection);

        return createForwardOnlyResultSetTable(tableName, preparedStatement, connection);
    }

    /**
     * Creates a new {@link ForwardOnlyResultSetTable} using the given
     * {@link PreparedStatement} to retrieve the data.
     *
     * @param tableName         The name the {@link ITable} will have
     * @param preparedStatement The statement which provides the data
     * @param connection        The database connection which also holds dbunit
     *                          configuration information
     * @return The new table object
     * @throws SQLException
     * @throws DataSetException
     */
    ForwardOnlyResultSetTable createForwardOnlyResultSetTable(String tableName, PreparedStatement preparedStatement,
            IDatabaseConnection connection) throws SQLException, DataSetException {
        DatabaseConfig config = connection.getDatabaseConfig();
        preparedStatement.setFetchSize(config.getFetchSize());

        ResultSet rs = preparedStatement.executeQuery();

        boolean caseSensitiveTableNames = config.isCaseSensitiveTableNames();
        ITableMetaData metaData = new ResultSetTableMetaData(tableName, rs, connection, caseSensitiveTableNames);
        ForwardOnlyResultSetTable table = new ForwardOnlyResultSetTable(metaData, rs);
        return table;
    }

}
