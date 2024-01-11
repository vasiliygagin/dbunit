/*
 *
 * The DbUnit Database Testing Framework
 * Copyright (C)2005, DbUnit.org
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.util.TreeSet;

import org.dbunit.AbstractDatabaseTest;
import org.dbunit.DdlExecutor;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.database.PrimaryKeyFilter.PkTableMap;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.NoSuchTableException;
import org.dbunit.util.search.SearchException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Felipe Leme (dbunit@felipeal.net)
 * @author Last changed by: $Author$
 * @version $Revision$ $Date$
 * @since Aug 28, 2005
 */
public class TablesDependencyHelperTest extends AbstractDatabaseTest {

    private IDatabaseConnection connection;

    public TablesDependencyHelperTest() throws Exception {
    }

    protected void setUp(String... sqlFiles) throws Exception {
        for (String element : sqlFiles) {
            File sql = new File("src/test/resources/sql/" + element);
            final File ddlFile = sql;
            DdlExecutor.executeDdlFile(database, ddlFile);
        }
    }

    @Before
    public void setUp() throws Exception {
        this.connection = new DatabaseConnection(database.getJdbcConnection(), environment.getDatabaseConfig());
    }

    @Test
    public void testGetDependentTablesFromOneTable() throws Exception {
        setUp("hypersonic_import.sql");
        String[][] allInput = ImportNodesFilterSearchCallbackTest.SINGLE_INPUT;
        String[][] allExpectedOutput = ImportNodesFilterSearchCallbackTest.SINGLE_OUTPUT;
        for (int i = 0; i < allInput.length; i++) {
            String[] input = allInput[i];
            String[] expectedOutput = allExpectedOutput[i];
            String[] actualOutput = TablesDependencyHelper.getDependentTables(this.connection, input[0]);
            Assert.assertArrayEquals("output didn't match for i=" + i, expectedOutput, actualOutput);
        }
    }

    @Test
    public void testGetDependentTablesFromOneTable_RootTableDoesNotExist() throws Exception {
        setUp("hypersonic_import.sql");

        try {
            TablesDependencyHelper.getDependentTables(this.connection, "XXXXXX_TABLE_NON_EXISTING");
            fail("Should not be able to get the dependent tables for a non existing input table");
        } catch (SearchException expected) {
            Throwable cause = expected.getCause();
            assertTrue(cause instanceof NoSuchTableException);
            String expectedMessage = "The table 'XXXXXX_TABLE_NON_EXISTING' does not exist in schema 'null'";
            assertEquals(expectedMessage, cause.getMessage());
        }
    }

    @Test
    public void testGetDependentTablesFromManyTables() throws Exception {
        setUp("hypersonic_import.sql");
        String[][] allInput = ImportNodesFilterSearchCallbackTest.COMPOUND_INPUT;
        String[][] allExpectedOutput = ImportNodesFilterSearchCallbackTest.COMPOUND_OUTPUT;
        for (int i = 0; i < allInput.length; i++) {
            String[] input = allInput[i];
            String[] expectedOutput = allExpectedOutput[i];
            String[] actualOutput = TablesDependencyHelper.getDependentTables(this.connection, input);
            Assert.assertArrayEquals("output didn't match for i=" + i, expectedOutput, actualOutput);
        }
    }

    @Test
    public void testGetAllDependentTablesFromOneTable() throws Exception {
        setUp("hypersonic_import_export.sql");
        String[][] allInput = ImportAndExportKeysSearchCallbackOwnFileTest.SINGLE_INPUT;
        String[][] allExpectedOutput = ImportAndExportKeysSearchCallbackOwnFileTest.SINGLE_OUTPUT;
        for (int i = 0; i < allInput.length; i++) {
            String[] input = allInput[i];
            String[] expectedOutput = allExpectedOutput[i];
            String[] actualOutput = TablesDependencyHelper.getAllDependentTables(this.connection, input[0]);
            Assert.assertArrayEquals("output didn't match for i=" + i, expectedOutput, actualOutput);
        }
    }

    @Test
    public void testGetAllDependentTablesFromManyTables() throws Exception {
        setUp(ImportAndExportKeysSearchCallbackOwnFileTest.SQL_FILE);
        String[][] allInput = ImportAndExportKeysSearchCallbackOwnFileTest.COMPOUND_INPUT;
        String[][] allExpectedOutput = ImportAndExportKeysSearchCallbackOwnFileTest.COMPOUND_OUTPUT;
        for (int i = 0; i < allInput.length; i++) {
            String[] input = allInput[i];
            String[] expectedOutput = allExpectedOutput[i];
            String[] actualOutput = TablesDependencyHelper.getAllDependentTables(this.connection, input);
            Assert.assertArrayEquals("output didn't match for i=" + i, expectedOutput, actualOutput);
        }
    }

    /**
     * Ensure the order is not lost on the way because of the conversion between Map
     * and Array
     *
     * @throws Exception
     */
    @Test
    public void testGetDatasetFromManyTables() throws Exception {
        setUp(ImportNodesFilterSearchCallbackTest.SQL_FILE);
        String[][] allInput = ImportNodesFilterSearchCallbackTest.COMPOUND_INPUT;
        String[][] allExpectedOutput = ImportNodesFilterSearchCallbackTest.COMPOUND_OUTPUT;
        for (int i = 0; i < allInput.length; i++) {
            String[] input = allInput[i];
            PkTableMap inputMap = new PkTableMap();
            for (String element : input) {
                inputMap.put(element, new TreeSet());
            }

            String[] expectedOutput = allExpectedOutput[i];
            IDataSet actualOutput = TablesDependencyHelper.getDataset(this.connection, inputMap);
            String[] actualOutputArray = actualOutput.getTableNames();
            Assert.assertArrayEquals("output didn't match for i=" + i, expectedOutput, actualOutputArray);
        }
    }

    // TODO ImportAndExportKeysSearchCallbackOwnFileTest

}
