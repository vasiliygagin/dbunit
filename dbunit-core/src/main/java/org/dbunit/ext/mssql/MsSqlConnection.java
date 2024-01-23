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
package org.dbunit.ext.mssql;

import java.sql.Connection;
import java.sql.SQLException;

import org.dbunit.DatabaseUnitException;
import org.dbunit.database.DatabaseConfig;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.metadata.MetadataManager;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.FilteredDataSet;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.filter.ExcludeTableFilter;
import org.dbunit.dataset.filter.ITableFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Manuel Laflamme
 * @since May 19, 2003
 * @version $Revision$
 */
public class MsSqlConnection extends DatabaseConnection {

    /**
     * Logger for this class
     */
    private static final Logger logger = LoggerFactory.getLogger(MsSqlConnection.class);

    private final ITableFilter _filter = new ExcludeTableFilter(new String[] { "dtproperties" });

    /**
     * Creates a new <code>MsSqlConnection</code>.
     *
     * @param connection the adapted JDBC connection
     * @param schema     the database schema
     * @param metadataManager
     * @throws DatabaseUnitException
     */
    public MsSqlConnection(Connection connection, String schema, MetadataManager metadataManager)
            throws DatabaseUnitException {
        super(connection, buildConfig(), schema, metadataManager);
    }

    static DatabaseConfig buildConfig() {
        DatabaseConfig config = new DatabaseConfig();
        config.setDataTypeFactory(new MsSqlDataTypeFactory());
        return config;
    }

    /**
     * Creates a new <code>MsSqlConnection</code>.
     *
     * @param connection the adapted JDBC connection
     * @param metadataManager
     * @throws DatabaseUnitException
     */
    public MsSqlConnection(Connection connection, MetadataManager metadataManager) throws DatabaseUnitException {
        super(connection, buildConfig(), metadataManager);
    }

    ////////////////////////////////////////////////////////////////////////////
    // IDatabaseConnection

    @Override
    public IDataSet createDataSet() throws SQLException, DataSetException {
        logger.debug("createDataSet() - start");

        IDataSet dataSet = super.createDataSet();
        return new FilteredDataSet(_filter, dataSet);
    }

    @Override
    public IDataSet createDataSet(String[] tableNames) throws SQLException, DataSetException {
        logger.debug("createDataSet(tableNames={}) - start", tableNames);

        IDataSet dataSet = super.createDataSet(tableNames);
        return new FilteredDataSet(_filter, dataSet);
    }
}
