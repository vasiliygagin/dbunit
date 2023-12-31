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
    private final DataSource _dataSource;
    private final String _user;
    private final String _password;
    private Connection _connection;

    public DatabaseDataSourceConnection(InitialContext context, String jndiName, String schema) throws NamingException {
        this((DataSource) context.lookup(jndiName), new DatabaseConfig(), schema, null, null);
    }

    public DatabaseDataSourceConnection(InitialContext context, String jndiName, String schema, String user,
            String password) throws NamingException {
        this((DataSource) context.lookup(jndiName), new DatabaseConfig(), schema, user, password);
    }

    public DatabaseDataSourceConnection(InitialContext context, String jndiName) throws NamingException {
        this(context, jndiName, null);
    }

    public DatabaseDataSourceConnection(InitialContext context, String jndiName, String user, String password)
            throws NamingException {
        this(context, jndiName, null, user, password);
    }

    public DatabaseDataSourceConnection(DataSource dataSource) {
        this(dataSource, new DatabaseConfig(), null, null, null);
    }

    public DatabaseDataSourceConnection(DataSource dataSource, String user, String password) {
        this(dataSource, new DatabaseConfig(), null, user, password);
    }

    public DatabaseDataSourceConnection(DataSource dataSource, String schema) {
        this(dataSource, new DatabaseConfig(), schema, null, null);
    }

    public DatabaseDataSourceConnection(DataSource dataSource, DatabaseConfig config, String schema, String user,
            String password) {
        super(dataSource, config, null, schema);
        _dataSource = dataSource;
        _schema = schema;
        _user = user;
        _password = password;
    }

    ////////////////////////////////////////////////////////////////////////////
    // IDatabaseConnection interface

    @Override
    public Connection getConnection() throws SQLException {
        logger.debug("getConnection() - start");

        if (_connection == null) {
            try {
                if (_user != null) {
                    _connection = _dataSource.getConnection(_user, _password);
                } else {
                    _connection = _dataSource.getConnection();
                }
            } catch (SQLException e) {
                logger.error("getConnection(): ", e);
                throw e;
            }
        }
        return _connection;
    }

    @Override
    public String getSchema() {
        return _schema;
    }

    @Override
    public void close() throws SQLException {
        logger.debug("close() - start");

        if (_connection != null) {
            _connection.close();
            _connection = null;
        }
    }
}
