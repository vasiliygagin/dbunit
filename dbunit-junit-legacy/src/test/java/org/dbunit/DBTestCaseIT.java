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
package org.dbunit;

import java.io.File;
import java.sql.SQLException;

import org.dbunit.database.DatabaseConfig;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.xml.FlatXmlDataSetBuilder;
import org.dbunit.operation.DatabaseOperation;
import org.dbunit.testutil.TestUtils;

import junit.framework.TestCase;

/**
 * @author gommma
 * @author Last changed by: $Author: gommma $
 * @version $Revision: 789 $ $Date: 2008-08-15 16:45:18 +0200 (Fr, 15. Aug 2008)
 *          $
 * @since 2.4.3
 */
public class DBTestCaseIT extends TestCase {

    public static final File DATASET_FILE = TestUtils.getFile("xml/flatXmlDataSetTest.xml");

    /**
     * Tests whether the user can simply change the {@link DatabaseConfig} by
     * overriding the method
     * {@link DatabaseTestCase#setUpDatabaseConfig(DatabaseConfig)}.
     *
     * @throws Exception
     */
    public void testConfigureConnection() throws Exception {
        DatabaseEnvironment dbEnv = DatabaseEnvironmentLoader.getInstance();
        final IDatabaseConnection conn = dbEnv.getConnection();
        final DefaultDatabaseTester tester = new DefaultDatabaseTester(conn);
        final DatabaseOperation operation = new DatabaseOperation() {
            @Override
            public void execute(IDatabaseConnection connection, IDataSet dataSet)
                    throws DatabaseUnitException, SQLException {
                assertEquals(97, connection.getDatabaseConfig().getBatchSize());
                assertEquals(true, connection.getDatabaseConfig().isBatchedStatements());
            }
        };

        DBTestCase testSubject = new DBTestCase() {

            /**
             * method under test
             */
            @Override
            protected void setUpDatabaseConfig(DatabaseConfig config) {
                config.setBatchSize(97);
                config.setBatchedStatements(true);
            }

            @Override
            protected IDatabaseTester newDatabaseTester() throws Exception {
                return tester;
            }

            @Override
            protected DatabaseOperation getSetUpOperation() throws Exception {
                return operation;
            }

            @Override
            protected DatabaseOperation getTearDownOperation() throws Exception {
                return operation;
            }

            @Override
            protected IDataSet getDataSet() throws Exception {
                return null;
            }
        };

        // Simulate JUnit which first of all calls the "setUp" method
        testSubject.setUp();

        IDatabaseConnection actualConn = testSubject.getConnection();
        assertEquals(97, actualConn.getDatabaseConfig().getBatchSize());
        assertSame(conn, actualConn);

        IDatabaseConnection actualConn2 = testSubject.getDatabaseTester().getConnection();
        assertEquals(97, actualConn2.getDatabaseConfig().getBatchSize());
        assertSame(tester, testSubject.getDatabaseTester());
        assertSame(conn, testSubject.getDatabaseTester().getConnection());
    }

    /**
     * Tests the simple setup/teardown invocations while keeping the
     * DatabaseConnection open.
     *
     * @throws Exception
     */
    public void testExecuteSetUpTearDown() throws Exception {
        // TODO implement this
        DatabaseEnvironment dbEnv = DatabaseEnvironmentLoader.getInstance();
        // Retrieve one single connection which is
        final IDatabaseConnection conn = dbEnv.getConnection();
        try {
            final DefaultDatabaseTester tester = new DefaultDatabaseTester(conn);
            final IDataSet dataset = new FlatXmlDataSetBuilder().build(DATASET_FILE);

            // Connection should not be closed during setUp/tearDown because of userDefined
            // IOperationListener
            DBTestCase testSubject = new DBTestCase() {

                @Override
                protected IDatabaseTester newDatabaseTester() throws Exception {
                    return tester;
                }

                @Override
                protected DatabaseOperation getSetUpOperation() throws Exception {
                    return DatabaseOperation.CLEAN_INSERT;
                }

                @Override
                protected DatabaseOperation getTearDownOperation() throws Exception {
                    return DatabaseOperation.DELETE_ALL;
                }

                @Override
                protected IDataSet getDataSet() throws Exception {
                    return dataset;
                }

                @Override
                protected IOperationListener getOperationListener() {
                    return new DefaultOperationListener() {
                        @Override
                        public void operationSetUpFinished(IDatabaseConnection connection) {
                            // Do not invoke the "super" method to avoid that the connection is closed
                            // Just do nothing
                        }

                        @Override
                        public void operationTearDownFinished(IDatabaseConnection connection) {
                            // Do not invoke the "super" method to avoid that the connection is closed
                            // Just do nothing
                        }

                    };
                }

            };

            // Simulate JUnit which first of all calls the "setUp" method
            testSubject.setUp();
            // The connection should still be open so we should be able to select from the
            // DB
            ITable testTableAfterSetup = conn.createTable("TEST_TABLE");
            assertEquals(6, testTableAfterSetup.getRowCount());
            assertFalse(conn.getConnection().isClosed());

            // Simulate JUnit and invoke "tearDown"
            testSubject.tearDown();
            // The connection should still be open so we should be able to select from the
            // DB
            ITable testTableAfterTearDown = conn.createTable("TEST_TABLE");
            assertEquals(0, testTableAfterTearDown.getRowCount());
            assertFalse(conn.getConnection().isClosed());
        } finally {
            // Ensure that the connection is closed again so that
            // it can be established later by subsequent test cases
            dbEnv.closeConnection();
        }
    }

}