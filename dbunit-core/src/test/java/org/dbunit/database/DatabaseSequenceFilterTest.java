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
import static org.junit.Assume.assumeTrue;

import java.io.File;
import java.sql.Connection;
import java.util.Arrays;

import org.dbunit.AbstractDatabaseTest;
import org.dbunit.CaseSensitive;
import org.dbunit.Database;
import org.dbunit.DdlExecutor;
import org.dbunit.H2Environment;
import org.dbunit.HsqldbEnvironment;
import org.dbunit.database.metadata.MetadataManager;
import org.dbunit.dataset.FilteredDataSet;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.filter.ITableFilter;
import org.dbunit.internal.connections.DriverManagerConnectionSource;
import org.dbunit.junit.internal.GlobalContext;
import org.junit.Test;

/**
 * @author Manuel Laflamme
 * @since May 8, 2004
 * @version $Revision$
 */
public class DatabaseSequenceFilterTest extends AbstractDatabaseTest {

    public DatabaseSequenceFilterTest() throws Exception {
    }

    @Override
    protected boolean checkEnvironment() {
        return environment instanceof HsqldbEnvironment;
    }

    @Override
    protected Database doOpenDatabase() throws Exception {
        return environment.openDatabase("tempdb", getconfigCustomizers());
    }

    @Test
    public void testGetTableNames() throws Exception {
        final String[] expectedNoFilter = { "A", "B", "C", "D", "E", "F", "G", "H", };
        final String[] expectedFiltered = { "D", "A", "F", "C", "G", "E", "H", "B", };

        dbUnit.executeSqlScript("src/test/resources/sql/hypersonic_fk.sql");

        DatabaseConnection connection = database.getConnection();
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

        final boolean multiLineSupport = environment.getProfileMultilineSupport();

        final Connection jdbcConnection = database.getJdbcConnection();
        DdlExecutor.executeDdlFile(ddlFile, jdbcConnection, multiLineSupport);

        DatabaseConnection connection = database.getConnection();
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
    @CaseSensitive(true)
    public void testCaseSensitiveTableNames() throws Exception {
        final String[] expectedNoFilter = { "MixedCaseTable", "UPPER_CASE_TABLE" };
        final String[] expectedFiltered = { "MixedCaseTable", "UPPER_CASE_TABLE" };

        dbUnit.executeSqlScript("src/test/resources/sql/hypersonic_case_sensitive_test.sql");

        DatabaseConnection connection = database.getConnection();

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
        assumeTrue(environment instanceof H2Environment);
        // TODO should be able to run this with hsqldb and others
        DriverManagerConnectionSource driverManagerConnectionSource = GlobalContext.getIt()
                .getDriverManagerConnectionSource();
        final Connection jdbcConnection = driverManagerConnectionSource.fetchConnection("org.h2.Driver",
                "jdbc:h2:mem:test", "sa", "");
        dbUnit.executeSqlScript("src/test/resources/sql/h2_multischema_fk_test.sql");
        DatabaseConfig config = new DatabaseConfig();
        config.setQualifiedTableNames(true);
        MetadataManager metadataManager = new MetadataManager(jdbcConnection, config, null, null);
        final DatabaseConnection connection = new DatabaseConnection(jdbcConnection, config, metadataManager);

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
