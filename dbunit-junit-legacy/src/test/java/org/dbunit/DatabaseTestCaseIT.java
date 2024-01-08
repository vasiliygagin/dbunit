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
import org.dbunit.operation.DatabaseOperation;

import junit.framework.TestCase;

/**
 * @author gommma
 * @author Last changed by: $Author$
 * @version $Revision$ $Date$
 * @since 2.3.0
 */
public class DatabaseTestCaseIT extends TestCase {

    public void testTearDownExceptionDoesNotObscureTestException() {
        // TODO implement #1087040 tearDownOperation Exception obscures underlying
        // problem
    }

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

        DatabaseTestCase testSubject = new DatabaseTestCase() {

            /**
             * method under test
             */
            @Override
            protected void setUpDatabaseConfig(DatabaseConfig config) {
                config.setBatchSize(97);
            }

            @Override
            protected IDatabaseConnection getConnection() throws Exception {
                return conn;
            }

            @Override
            protected IDataSet getDataSet() throws Exception {
                return null;
            }

            @Override
            protected DatabaseOperation getSetUpOperation() throws Exception {
                return DatabaseOperation.NONE;
            }

            @Override
            protected DatabaseOperation getTearDownOperation() throws Exception {
                return DatabaseOperation.NONE;
            }
        };

        // Simulate JUnit which first of all calls the "setUp" method
        testSubject.setUp();

        IDatabaseConnection actualConn = testSubject.getConnection();
        assertEquals(97, actualConn.getDatabaseConfig().getBatchSize());

        IDatabaseConnection actualConn2 = testSubject.getDatabaseTester().getConnection();
        assertEquals(97, actualConn2.getDatabaseConfig().getBatchSize());
    }
}
