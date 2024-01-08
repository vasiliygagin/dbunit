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
package org.dbunit.database.search;

import static org.junit.Assert.assertArrayEquals;

import java.sql.Connection;
import java.util.Set;

import org.dbunit.DdlExecutor;
import org.dbunit.HypersonicEnvironment;
import org.dbunit.database.DatabaseConfig;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.internal.connections.DriverManagerConnectionsFactory;
import org.dbunit.testutil.TestUtils;
import org.dbunit.util.CollectionsHelper;
import org.dbunit.util.search.DepthFirstSearch;
import org.dbunit.util.search.ISearchCallback;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Felipe Leme (dbunit@felipeal.net)
 * @version $Revision$
 * @since Aug 28, 2005
 */
public abstract class AbstractMetaDataBasedSearchCallbackTestCase {

    private final String sqlFile;

    private Connection jdbcConnection;

    private IDatabaseConnection connection;

    public AbstractMetaDataBasedSearchCallbackTestCase(String sqlFile) {
        this.sqlFile = sqlFile;
    }

    @Before
    public final void setUp() throws Exception {
        this.jdbcConnection = DriverManagerConnectionsFactory.getIT().fetchConnection("org.hsqldb.jdbcDriver",
        "jdbc:hsqldb:mem:" + "tempdb", "sa", "");
        DdlExecutor.executeDdlFile(TestUtils.getFile("sql/" + this.sqlFile), this.jdbcConnection);
        this.connection = new DatabaseConnection(jdbcConnection, new DatabaseConfig());
    }

    @After
    public final void tearDown() throws Exception {
        DdlExecutor.executeSql(this.jdbcConnection, "DROP SCHEMA PUBLIC IF EXISTS CASCADE");
        DdlExecutor.executeSql(this.jdbcConnection, "DROP SCHEMA TEST_SCHEMA IF EXISTS CASCADE");
        DdlExecutor.executeSql(this.jdbcConnection, "SET SCHEMA PUBLIC");
        this.jdbcConnection.close();
//     HypersonicEnvironment.deleteFiles( "tempdb" );
    }

    protected IDatabaseConnection getConnection() {
        return this.connection;
    }

    protected abstract String[][] getInput();

    protected abstract String[][] getExpectedOutput();

    protected abstract AbstractMetaDataBasedSearchCallback getCallback(IDatabaseConnection connection2);

    @Test
    public void testAllInput() throws Exception {
        IDatabaseConnection connection = getConnection();

        String[][] allInput = getInput();
        String[][] allExpectedOutput = getExpectedOutput();
        ISearchCallback callback = getCallback(connection);
        for (int i = 0; i < allInput.length; i++) {
            String[] input = allInput[i];
            String[] expectedOutput = allExpectedOutput[i];
            DepthFirstSearch search = new DepthFirstSearch();
            Set result = search.search(input, callback);
            String[] actualOutput = CollectionsHelper.setToStrings(result);
            assertArrayEquals("output didn't match for i=" + i, expectedOutput, actualOutput);
        }
    }

}
