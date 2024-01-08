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

package org.dbunit;

import java.io.File;
import java.io.FileReader;
import java.sql.Connection;

import org.dbunit.database.DatabaseConnection;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.xml.XmlDataSet;
import org.dbunit.internal.connections.DriverManagerConnectionsFactory;

import io.github.vasiliygagin.dbunit.jdbc.DatabaseConfig;

/**
 * @author Manuel Laflamme
 * @version $Revision$
 * @since Feb 18, 2002
 */
public class DatabaseEnvironment {

    private final DatabaseProfile _profile;
    private final Connection jdbcConnection;
    private final IDataSet _dataSet;
    private final JdbcDatabaseTester _databaseTester;
    private final DatabaseConfig databaseConfig;

    private DatabaseConnection _connection = null;

    protected DatabaseEnvironment(final DatabaseProfile profile, DatabaseConfig databaseConfig) throws Exception {
        _profile = profile;
        jdbcConnection = buildJdbcConnection();
        _dataSet = new XmlDataSet(new FileReader(new File("src/test/resources/xml/dataSetTest.xml")));
        _databaseTester = new JdbcDatabaseTester(_profile.getDriverClass(), _profile.getConnectionUrl(),
                _profile.getUser(), _profile.getPassword(), _profile.getSchema());
        this.databaseConfig = databaseConfig;
        _connection = getConnection();

        DdlExecutor.execute("sql/" + _profile.getProfileDdl(), _connection.getConnection(),
                profile.getProfileMultilineSupport(), true);
    }

    private Connection buildJdbcConnection() {
        return DriverManagerConnectionsFactory.getIT().fetchConnection(_profile.getDriverClass(),
                _profile.getConnectionUrl(), _profile.getUser(), _profile.getPassword());
    }

    public DatabaseConfig getDatabaseConfig() {
        return databaseConfig;
    }

    public DatabaseConnection getConnection() throws Exception {
        // First check if the current connection is still valid and open
        // The connection may have been closed by a consumer
        if (_connection != null && _connection.getConnection().isClosed()) {
            // Reset the member so that a new connection will be created
            _connection = null;
        }

        if (_connection == null) {
            _connection = new DatabaseConnection(jdbcConnection, databaseConfig, _profile.getSchema());
        }
        return _connection;
    }

    public final Connection fetchJdbcConnection() {
        return jdbcConnection;
    }

    public final JdbcDatabaseTester getDatabaseTester() {
        return _databaseTester;
    }

    public void closeConnection() throws Exception {
        if (_connection != null) {
            _connection.close();
            _connection = null;
        }
    }

    public IDataSet getInitDataSet() throws Exception {
        return _dataSet;
    }

    public DatabaseProfile getProfile() throws Exception {
        return _profile;
    }

    public boolean support(final TestFeature feature) {
        final String[] unsupportedFeatures = _profile.getUnsupportedFeatures();
        for (final String unsupportedFeature : unsupportedFeatures) {
            if (feature.toString().equals(unsupportedFeature)) {
                return false;
            }
        }

        return true;
    }

    /**
     * Returns the string converted as an identifier according to the metadata rules
     * of the database environment. Most databases convert all metadata identifiers
     * to uppercase. PostgreSQL converts identifiers to lowercase. MySQL preserves
     * case.
     *
     * @param str The identifier.
     * @return The identifier converted according to database rules.
     */
    public String convertString(final String str) {
        return str == null ? null : str.toUpperCase();
    }
}
