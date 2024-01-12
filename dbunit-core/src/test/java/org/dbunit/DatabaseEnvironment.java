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
import java.sql.DriverManager;
import java.sql.SQLException;

import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.xml.XmlDataSet;

import io.github.vasiliygagin.dbunit.jdbc.DatabaseConfig;

/**
 * @author Manuel Laflamme
 * @version $Revision$
 * @since Feb 18, 2002
 */
public abstract class DatabaseEnvironment {

    private final String defaultDatabaseName;
    private final String schema;
    private final boolean multilineSupport;
    private final String[] unsupportedFeatures;
    private final DatabaseConfig databaseConfig;

    private final IDataSet _dataSet;

    // temp until things are cleaned
    private Database openedDatabase;
    private DatabaseProfile profile;

    protected DatabaseEnvironment(String defaultDatabaseName, final DatabaseProfile profile,
            DatabaseConfig databaseConfig) throws Exception {
        this.defaultDatabaseName = defaultDatabaseName;
        this.databaseConfig = databaseConfig;
        this.profile = profile;
        schema = profile.getSchema();
        multilineSupport = profile.getProfileMultilineSupport();
        unsupportedFeatures = profile.getUnsupportedFeatures();
        _dataSet = new XmlDataSet(new FileReader(new File("src/test/resources/xml/dataSetTest.xml").getAbsoluteFile()));
        this.databaseConfig.freese();
    }

    protected abstract String buildConnectionUrl(String databaseName);

    @Deprecated
    public Database getOpenedDatabase() {
        return openedDatabase;
    }

    public Database openDefaultDatabase() throws Exception {
        Database database = openDatabase(defaultDatabaseName);
        DdlExecutor.execute("sql/" + profile.getProfileDdl(), database.getJdbcConnection(), false, false);
        return database;
    }

    public Database openDatabase(String databaseName) throws Exception {
        Connection jdbcConnection = buildJdbcConnection(databaseName);

        JdbcDatabaseTester databaseTester = new JdbcDatabaseTester(jdbcConnection, schema);
        databaseTester.setOperationListener(new DefaultOperationListener() {

            @Override
            public void operationSetUpFinished(IDatabaseConnection connection) {
            }

            @Override
            public void operationTearDownFinished(IDatabaseConnection connection) {
            }
        });

        DatabaseConnection connection = new DatabaseConnection(jdbcConnection, databaseConfig, schema);

        Database database = new Database(this);
        database.setJdbcConnection(jdbcConnection);
        database.setConnection(connection);
        database.setDatabaseTester(databaseTester);
        return database;
    }

    private Connection buildJdbcConnection(String databaseName) {
        String connectionUrl = buildConnectionUrl(databaseName);
        Connection connection1;
        try {
            Class.forName(profile.getDriverClass());
            connection1 = openConnection(connectionUrl);
            connection1.setAutoCommit(false);
        } catch (ClassNotFoundException | SQLException exc) {
            throw new AssertionError(" Unable to connect to [" + connectionUrl + "]", exc);
        }
//        return DriverManagerConnectionsFactory.getIT().fetchConnection(profile.getDriverClass(), connectionUrl,
//                profile.getUser(), profile.getPassword());
        return connection1;
    }

    protected Connection openConnection(String connectionUrl) throws SQLException {
        return DriverManager.getConnection(connectionUrl, profile.getUser(), profile.getPassword());
    }

    public void closeDatabase(Database database) {

    }

    public final DatabaseConfig getDatabaseConfig() {
        return databaseConfig;
    }

    @Deprecated
    public void closeConnection() throws Exception {
        // ?? should closeDatabase be used instead?
//        if (_connection != null) {
//            _connection.close();
//            _connection = null;
//        }
    }

    public IDataSet getInitDataSet() throws Exception {
        return _dataSet;
    }

    public String getSchema() {
        return schema;
    }

    public boolean getProfileMultilineSupport() {
        return multilineSupport;
    }

    public boolean support(final TestFeature feature) {
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
