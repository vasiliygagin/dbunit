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

import org.dbunit.database.DatabaseConfig;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.xml.XmlDataSet;
import org.dbunit.testutil.TestUtils;

/**
 * @author Manuel Laflamme
 * @version $Revision$
 * @since Feb 18, 2002
 */
public class DatabaseEnvironment {

    private DatabaseProfile _profile = null;
    private IDatabaseConnection _connection = null;
    private IDataSet _dataSet = null;
    private IDatabaseTester _databaseTester = null;

    protected DatabaseEnvironment(final DatabaseProfile profile) throws Exception {
	_profile = profile;
	final File file = TestUtils.getFile("xml/dataSetTest.xml");
	_dataSet = new XmlDataSet(new FileReader(file));
	_databaseTester = new JdbcDatabaseTester(_profile.getDriverClass(), _profile.getConnectionUrl(),
		_profile.getUser(), _profile.getPassword(), _profile.getSchema());

	DdlExecutor.execute("sql/" + _profile.getProfileDdl(), getConnection().getConnection(),
		profile.getProfileMultilineSupport(), true);
    }

    public IDatabaseConnection getConnection() throws Exception {
	// First check if the current connection is still valid and open
	// The connection may have been closed by a consumer
	if (_connection != null && _connection.getConnection().isClosed()) {
	    // Reset the member so that a new connection will be created
	    _connection = null;
	}

	if (_connection == null) {
	    final String name = _profile.getDriverClass();
	    Class.forName(name);
	    final Connection connection = DriverManager.getConnection(_profile.getConnectionUrl(), _profile.getUser(),
		    _profile.getPassword());
	    _connection = new DatabaseConnection(connection, _profile.getSchema());
	}
	return _connection;
    }

    protected void setupDatabaseConfig(final DatabaseConfig config) {
	// Override in subclasses as necessary.
    }

    public IDatabaseTester getDatabaseTester() {
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
	for (int i = 0; i < unsupportedFeatures.length; i++) {
	    final String unsupportedFeature = unsupportedFeatures[i];
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

    @Override
    public String toString() {
	final StringBuffer sb = new StringBuffer();
	sb.append(getClass().getName()).append("[");
	sb.append("_profile=").append(_profile);
	sb.append(", _connection=").append(_connection);
	sb.append(", _dataSet=").append(_dataSet);
	sb.append(", _databaseTester=").append(_databaseTester);
	sb.append("]");
	return sb.toString();
    }
}
