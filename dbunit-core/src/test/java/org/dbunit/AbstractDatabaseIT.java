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

import java.sql.Connection;

import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.SortedTable;
import org.dbunit.operation.DatabaseOperation;
import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.vasiliygagin.dbunit.jdbc.DatabaseConfig;

/**
 * @author Manuel Laflamme
 * @version $Revision$
 * @since Feb 18, 2002
 */
public abstract class AbstractDatabaseIT {

    protected IDatabaseConnection customizedConnection;

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    protected final DatabaseEnvironment environment;

    public AbstractDatabaseIT() throws Exception {
        environment = DatabaseEnvironmentLoader.getInstance();
    }

    @Before
    public final void setUp() throws Exception {

        JdbcDatabaseTester databaseTester = environment.getDatabaseTester();

        DatabaseConfig config = new DatabaseConfig();
        databaseTester.setDatabaseConfig(config);
        databaseTester.setSetUpOperation(getSetUpOperation());
        databaseTester.setDataSet(getDataSet());
        databaseTester.onSetup();

        Connection conn = databaseTester.buildJdbcConnection();
        customizedConnection = new DatabaseConnection(conn, config, databaseTester.getSchema());
    }

    @After
    public final void tearDown() throws Exception {
        final IDatabaseTester databaseTester = environment.getDatabaseTester();

        databaseTester.setTearDownOperation(getTearDownOperation());
        databaseTester.setDataSet(getDataSet());
        databaseTester.onTearDown();

        DatabaseOperation.DELETE_ALL.execute(customizedConnection, customizedConnection.createDataSet());

        customizedConnection.close();
        customizedConnection = null;
    }

    protected DatabaseEnvironment getEnvironment() throws Exception {
        return environment;
    }

    protected ITable createOrderedTable(String tableName, String orderByColumn) throws Exception {
        return new SortedTable(customizedConnection.createDataSet().getTable(tableName),
                new String[] { orderByColumn });
//        String sql = "select * from " + tableName + " order by " + orderByColumn;
//        return _connection.createQueryTable(tableName, sql);
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
    protected String convertString(String str) throws Exception {
        return environment.convertString(str);
    }

    protected IDataSet getDataSet() throws Exception {
        return environment.getInitDataSet();
    }

    protected void closeConnection(IDatabaseConnection connection) throws Exception {
    }

    /**
     * This method is used so sub-classes can disable the tests according to some
     * characteristics of the environment
     *
     * @param testName name of the test to be checked
     * @return flag indicating if the test should be executed or not
     */
    public final boolean runTest(String testName) {
        return true;
    }

    /**
     * Returns the database operation executed in test setup.
     */
    protected DatabaseOperation getSetUpOperation() throws Exception {
        return DatabaseOperation.CLEAN_INSERT;
    }

    /**
     * Returns the database operation executed in test cleanup.
     */
    protected DatabaseOperation getTearDownOperation() throws Exception {
        return DatabaseOperation.NONE;
    }
}
