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

import java.sql.Connection;
import java.sql.SQLException;

import javax.naming.InitialContext;
import javax.naming.NamingException;
import javax.sql.DataSource;

import org.dbunit.database.metadata.MetadataManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class adapts a JDBC <code>DataSource</code> to a
 * {@link IDatabaseConnection}.
 *
 * @author Manuel Laflamme, Vasiliy Gagin
 * @version $Revision$
 * @since Mar 8, 2002
 */
public class DatabaseDataSourceConnection extends AbstractDatabaseConnection {

    /**
     * Logger for this class
     */
    private static final Logger logger = LoggerFactory.getLogger(DatabaseDataSourceConnection.class);

    private final String _schema;

    public DatabaseDataSourceConnection(InitialContext context, String jndiName, String schema,
            MetadataManager metadataManager) throws NamingException, SQLException {
        this((DataSource) context.lookup(jndiName), new DatabaseConfig(), schema, null, null, metadataManager);
    }

    public DatabaseDataSourceConnection(InitialContext context, String jndiName, String schema, String user,
            String password, MetadataManager metadataManager) throws NamingException, SQLException {
        this((DataSource) context.lookup(jndiName), new DatabaseConfig(), schema, user, password, metadataManager);
    }

    public DatabaseDataSourceConnection(InitialContext context, String jndiName, MetadataManager metadataManager)
            throws NamingException, SQLException {
        this(context, jndiName, null, metadataManager);
    }

    public DatabaseDataSourceConnection(InitialContext context, String jndiName, String user, String password,
            MetadataManager metadtaManager) throws NamingException, SQLException {
        this(context, jndiName, null, user, password, metadtaManager);
    }

    public DatabaseDataSourceConnection(DataSource dataSource, MetadataManager metadataManager) throws SQLException {
        this(dataSource, new DatabaseConfig(), null, null, null, metadataManager);
    }

    public DatabaseDataSourceConnection(DataSource dataSource, String user, String password,
            MetadataManager metedataManager) throws SQLException {
        this(dataSource, new DatabaseConfig(), null, user, password, metedataManager);
    }

    public DatabaseDataSourceConnection(DataSource dataSource, String schema, MetadataManager metadataManager)
            throws SQLException {
        this(dataSource, new DatabaseConfig(), schema, null, null, metadataManager);
    }

    public DatabaseDataSourceConnection(DataSource dataSource, DatabaseConfig config, String schema, String user,
            String password, MetadataManager metadataManager) throws SQLException {
        super(getConnection(dataSource, user, password), config, null, schema, metadataManager);
        _schema = schema;
    }

    private static Connection getConnection(DataSource dataSource, String user, String password) throws SQLException {
        if (user != null) {
            return dataSource.getConnection(user, password);
        } else {
            return dataSource.getConnection();
        }
    }

    ////////////////////////////////////////////////////////////////////////////
    // IDatabaseConnection interface

    @Override
    public String getSchema() {
        return _schema;
    }

    @Override
    public void close() throws SQLException {
    }
}
