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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.sql.Connection;
import java.util.Arrays;

import org.dbunit.DatabaseEnvironment;
import org.dbunit.DatabaseEnvironmentLoader;
import org.dbunit.DdlExecutor;
import org.dbunit.dataset.FilteredDataSet;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.filter.ITableFilter;
import org.dbunit.internal.connections.DriverManagerConnectionsFactory;
import org.dbunit.testutil.TestUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Manuel Laflamme
 * @since May 8, 2004
 * @version $Revision$
 */
public class DatabaseSequenceFilterTest {

    Connection _jdbcConnection;
    private final DatabaseEnvironment environment;

    public DatabaseSequenceFilterTest() throws Exception {
        environment = DatabaseEnvironmentLoader.getInstance();
    }

    @Before
    public final void setUp() throws Exception {
        _jdbcConnection = DriverManagerConnectionsFactory.getIT().fetchConnection("org.hsqldb.jdbcDriver",
                "jdbc:hsqldb:mem:" + "tempdb", "sa", "");
    }

    @After
    public final void tearDown() throws Exception {
        DdlExecutor.executeSql(_jdbcConnection, "DROP SCHEMA PUBLIC IF EXISTS CASCADE");
        DdlExecutor.executeSql(_jdbcConnection, "DROP SCHEMA TEST_SCHEMA IF EXISTS CASCADE");
        DdlExecutor.executeSql(_jdbcConnection, "SET SCHEMA PUBLIC");
    }

    @Test
    public void testGetTableNames() throws Exception {
        final String[] expectedNoFilter = { "A", "B", "C", "D", "E", "F", "G", "H", };
        final String[] expectedFiltered = { "D", "A", "F", "C", "G", "E", "H", "B", };
        final Connection connection1 = _jdbcConnection;

        DdlExecutor.executeDdlFile(environment, connection1, TestUtils.getFile("sql/hypersonic_fk.sql"));
        final IDatabaseConnection connection = new DatabaseConnection(_jdbcConnection, new DatabaseConfig());

        final IDataSet databaseDataset = connection.createDataSet();
        final String[] actualNoFilter = databaseDataset.getTableNames();
        assertEquals("no filter", Arrays.asList(expectedNoFilter), Arrays.asList(actualNoFilter));

        final ITableFilter filter = new DatabaseSequenceFilter(connection);
        final IDataSet filteredDataSet = new FilteredDataSet(filter, databaseDataset);
        final String[] actualFiltered = filteredDataSet.getTableNames();
        assertEquals("filtered", Arrays.asList(expectedFiltered), Arrays.asList(actualFiltered));
    }

    @Test
    public void testGetTableNamesCyclic() throws Exception {
        final String[] expectedNoFilter = { "A", "B", "C", "D", "E", };
        final File ddlFile = new File("src/test/resources/sql/hypersonic_cyclic.sql");
        final Connection connection1 = _jdbcConnection;

        final boolean multiLineSupport = environment.getProfileMultilineSupport();

        DdlExecutor.executeDdlFile(ddlFile, connection1, multiLineSupport);
        final IDatabaseConnection connection = new DatabaseConnection(_jdbcConnection, new DatabaseConfig());

        final IDataSet databaseDataset = connection.createDataSet();
        final String[] actualNoFilter = databaseDataset.getTableNames();
        assertEquals("no filter", Arrays.asList(expectedNoFilter), Arrays.asList(actualNoFilter));

        boolean gotCyclicTablesDependencyException = false;

        try {
            final ITableFilter filter = new DatabaseSequenceFilter(connection);
            final IDataSet filteredDataSet = new FilteredDataSet(filter, databaseDataset);
            filteredDataSet.getTableNames();
            fail("Should not be here!");
        } catch (final CyclicTablesDependencyException expected) {
            gotCyclicTablesDependencyException = true;
        }
        assertTrue("Expected CyclicTablesDependencyException was not raised", gotCyclicTablesDependencyException);
    }

    @Test
    public void testCaseSensitiveTableNames() throws Exception {
        final String[] expectedNoFilter = { "MixedCaseTable", "UPPER_CASE_TABLE" };
        final String[] expectedFiltered = { "MixedCaseTable", "UPPER_CASE_TABLE" };
        final Connection connection1 = _jdbcConnection;

        DdlExecutor.executeDdlFile(environment, connection1,
                TestUtils.getFile("sql/hypersonic_case_sensitive_test.sql"));
        DatabaseConfig config = new DatabaseConfig();
        config.setCaseSensitiveTableNames(true);
        final IDatabaseConnection connection = new DatabaseConnection(_jdbcConnection, config);

        final IDataSet databaseDataset = connection.createDataSet();
        final String[] actualNoFilter = databaseDataset.getTableNames();
        assertEquals("no filter", Arrays.asList(expectedNoFilter), Arrays.asList(actualNoFilter));

        final ITableFilter filter = new DatabaseSequenceFilter(connection);
        final IDataSet filteredDataSet = new FilteredDataSet(filter, databaseDataset);
        final String[] actualFiltered = filteredDataSet.getTableNames();
        assertEquals("filtered", Arrays.asList(expectedFiltered), Arrays.asList(actualFiltered));
    }

    /**
     * Note that this test uses the H2 database because we could not find out how to
     * create 2 separate schemas in the hsqldb in memory DB.
     *
     * @throws Exception
     */
    @Test
    public void testMultiSchemaFks() throws Exception {
        final Connection jdbcConnection = DriverManagerConnectionsFactory.getIT().fetchConnection("org.h2.Driver",
                "jdbc:h2:mem:test", "sa", "");
        DdlExecutor.executeDdlFile(environment, jdbcConnection, TestUtils.getFile("sql/h2_multischema_fk_test.sql"));
        DatabaseConfig config = new DatabaseConfig();
        config.setQualifiedTableNames(true);
        final IDatabaseConnection connection = new DatabaseConnection(jdbcConnection, config);

        final IDataSet databaseDataset = connection.createDataSet();
        final ITableFilter filter = new DatabaseSequenceFilter(connection);
        final IDataSet filteredDataSet = new FilteredDataSet(filter, databaseDataset);

        final String[] actualNoFilter = databaseDataset.getTableNames();
        assertEquals(2, actualNoFilter.length);
        assertEquals("A.FOO", actualNoFilter[0]);
        assertEquals("B.BAR", actualNoFilter[1]);

        final String[] actualFiltered = filteredDataSet.getTableNames();
        assertEquals(2, actualFiltered.length);
        assertEquals("A.FOO", actualFiltered[0]);
        assertEquals("B.BAR", actualFiltered[1]);
    }
}
