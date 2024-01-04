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

import org.dbunit.database.DatabaseConfig;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.SortedTable;
import org.dbunit.operation.DatabaseOperation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import junit.framework.TestCase;

/**
 * @author Manuel Laflamme
 * @version $Revision$
 * @since Feb 18, 2002
 */
public abstract class AbstractDatabaseIT extends TestCase {
    protected IDatabaseConnection _connection;

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    public AbstractDatabaseIT(String s) {
        super(s);
    }

    protected DatabaseEnvironment getEnvironment() throws Exception {
        return DatabaseEnvironmentLoader.getInstance(null);
    }

    protected ITable createOrderedTable(String tableName, String orderByColumn) throws Exception {
        return new SortedTable(_connection.createDataSet().getTable(tableName), new String[] { orderByColumn });
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
        return getEnvironment().convertString(str);
    }

    ////////////////////////////////////////////////////////////////////////////
    // TestCase class

    @Override
    protected void setUp() throws Exception {
        super_setUp();

        _connection = getDatabaseTester().getConnection();
        setUpDatabaseConfig(_connection.getConfig());
    }

    protected IDatabaseTester getDatabaseTester() throws Exception {
        try {
            return getEnvironment().getDatabaseTester();
        } catch (Exception e) {
            // TODO matthias: this here hides original exceptions from being shown in the
            // JUnit results
            // (logger is not configured for unit tests). Think about how exceptions can be
            // passed through
            // So I temporarily added the "e.printStackTrace()"...
            logger.error("getDatabaseTester()", e);
            e.printStackTrace();
        }
        return super_getDatabaseTester();
    }

    protected void setUpDatabaseConfig(DatabaseConfig config) {
        try {
            getEnvironment().setupDatabaseConfig(config);
        } catch (Exception ex) {
            throw new RuntimeException(ex); // JH_TODO: is this the "DbUnit way" to handle exceptions?
        }
    }

    @Override
    protected void tearDown() throws Exception {
        super_tearDown();

        DatabaseOperation.DELETE_ALL.execute(_connection, _connection.createDataSet());

        _connection.close();

        _connection = null;
    }

    ////////////////////////////////////////////////////////////////////////////
    // DatabaseTestCase class

    protected IDatabaseConnection getConnection() throws Exception {
        IDatabaseConnection connection = getEnvironment().getConnection();
        return connection;

//        return new DatabaseEnvironment(getEnvironment().getProfile()).getConnection();
//        return new DatabaseConnection(connection.getConnection(), connection.getSchema());
    }

    protected IDataSet getDataSet() throws Exception {
        return getEnvironment().getInitDataSet();
    }

    protected void closeConnection(IDatabaseConnection connection) throws Exception {
//        getEnvironment().closeConnection();
    }
//
//    protected DatabaseOperation getTearDownOperation() throws Exception
//    {
//        return DatabaseOperation.DELETE_ALL;
//    }

    /**
     * This method is used so sub-classes can disable the tests according to some
     * characteristics of the environment
     *
     * @param testName name of the test to be checked
     * @return flag indicating if the test should be executed or not
     */
    protected boolean runTest(String testName) {
        return true;
    }

    @Override
    protected void runTest() throws Throwable {
        if (runTest(getName())) {
            super.runTest();
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("Skipping test " + getClass().getName() + "." + getName());
            }
        }
    }

    public static boolean environmentHasFeature(TestFeature feature) {
        try {
            final DatabaseEnvironment environment = DatabaseEnvironmentLoader.getInstance(null);
            final boolean runIt = environment.support(feature);
            return runIt;
        } catch (Exception e) {
            throw new DatabaseUnitRuntimeException(e);
        }
    }

    private IDatabaseTester tester;

    private IOperationListener operationListener;

    /**
     * Creates a IDatabaseTester for this testCase.<br>
     *
     * A {@link DefaultDatabaseTester} is used by default.
     *
     * @throws Exception
     */
    protected IDatabaseTester newDatabaseTester() throws Exception {
        logger.debug("newDatabaseTester() - start");

        final IDatabaseConnection connection = getConnection();
        getOperationListener().connectionRetrieved(connection);
        final IDatabaseTester tester = new DefaultDatabaseTester(connection);
        return tester;
    }

    /**
     * Gets the IDatabaseTester for this testCase.<br>
     * If the IDatabaseTester is not set yet, this method calls newDatabaseTester()
     * to obtain a new instance.
     *
     * @throws Exception
     */
    protected IDatabaseTester super_getDatabaseTester() throws Exception {
        if (this.tester == null) {
            this.tester = newDatabaseTester();
        }
        return this.tester;
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

    ////////////////////////////////////////////////////////////////////////////
    // TestCase class

    protected void super_setUp() throws Exception {
        logger.debug("setUp() - start");

        super.setUp();
        final IDatabaseTester databaseTester = getDatabaseTester();
        assertNotNull("DatabaseTester is not set", databaseTester);
        databaseTester.setSetUpOperation(getSetUpOperation());
        databaseTester.setDataSet(getDataSet());
        databaseTester.setOperationListener(getOperationListener());
        databaseTester.onSetup();
    }

    protected void super_tearDown() throws Exception {
        logger.debug("tearDown() - start");

        try {
            final IDatabaseTester databaseTester = getDatabaseTester();
            assertNotNull("DatabaseTester is not set", databaseTester);
            databaseTester.setTearDownOperation(getTearDownOperation());
            databaseTester.setDataSet(getDataSet());
            databaseTester.setOperationListener(getOperationListener());
            databaseTester.onTearDown();
        } finally {
            tester = null;
            super.tearDown();
        }
    }

    /**
     * @return The {@link IOperationListener} to be used by the
     *         {@link IDatabaseTester}.
     * @since 2.4.4
     */
    protected IOperationListener getOperationListener() {
        logger.debug("getOperationListener() - start");
        if (this.operationListener == null) {
            this.operationListener = new DefaultOperationListener() {
                @Override
                public void connectionRetrieved(IDatabaseConnection connection) {
                    super.connectionRetrieved(connection);
                    // When a new connection has been created then invoke the setUp method
                    // so that user defined DatabaseConfig parameters can be set.
                    setUpDatabaseConfig(connection.getConfig());
                }
            };
        }
        return this.operationListener;
    }
}
