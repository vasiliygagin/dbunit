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

package org.dbunit.ext.mssql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import java.io.Reader;

import org.dbunit.AbstractDatabaseIT;
import org.dbunit.Assertion;
import org.dbunit.DatabaseTestingEnvironment;
import org.dbunit.DatabaseEnvironmentLoader;
import org.dbunit.TestFeature;
import org.dbunit.dataset.DataSetUtils;
import org.dbunit.dataset.ForwardOnlyDataSet;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.LowerCaseDataSet;
import org.dbunit.dataset.filter.IColumnFilter;
import org.dbunit.dataset.xml.FlatXmlDataSetBuilder;
import org.dbunit.dataset.xml.XmlDataSet;
import org.dbunit.operation.DatabaseOperation;
import org.dbunit.testutil.TestUtils;
import org.junit.Test;

/**
 * @author Manuel Laflamme
 * @author Eric Pugh
 * @version $Revision$
 * @since Feb 19, 2002
 */
public class InsertIdentityOperationIT extends AbstractDatabaseIT {

    final DatabaseTestingEnvironment environment;

    public InsertIdentityOperationIT() throws Exception {
        environment = DatabaseEnvironmentLoader.getInstance();
    }

    private boolean environmentHasInsertIdentityFeature() {
        return environment.support(TestFeature.INSERT_IDENTITY);
    }

    @Test
    public void testExecuteXML() throws Exception {
        assumeTrue(environmentHasInsertIdentityFeature());
        Reader in = TestUtils.getFileReader("xml/insertIdentityOperationTest.xml");
        IDataSet dataSet = new XmlDataSet(in);

        testExecute(dataSet);
    }

    @Test
    public void testExecuteFlatXML() throws Exception {
        assumeTrue(environmentHasInsertIdentityFeature());
        Reader in = TestUtils.getFileReader("xml/insertIdentityOperationTestFlat.xml");
        IDataSet dataSet = new FlatXmlDataSetBuilder().build(in);

        testExecute(dataSet);
    }

    @Test
    public void testExecuteLowerCase() throws Exception {
        assumeTrue(environmentHasInsertIdentityFeature());
        Reader in = TestUtils.getFileReader("xml/insertIdentityOperationTestFlat.xml");
        IDataSet dataSet = new LowerCaseDataSet(new FlatXmlDataSetBuilder().build(in));

        testExecute(dataSet);
    }

    @Test
    public void testExecuteForwardOnly() throws Exception {
        assumeTrue(environmentHasInsertIdentityFeature());
        Reader in = TestUtils.getFileReader("xml/insertIdentityOperationTestFlat.xml");
        IDataSet dataSet = new ForwardOnlyDataSet(new FlatXmlDataSetBuilder().build(in));

        testExecute(dataSet);
    }

    private void testExecute(IDataSet dataSet) throws Exception {
        ITable[] tablesBefore = DataSetUtils.getTables(customizedConnection.createDataSet());
//        InsertIdentityOperation.CLEAN_INSERT.execute(_connection, dataSet);
        InsertIdentityOperation.INSERT.execute(customizedConnection, dataSet);
        ITable[] tablesAfter = DataSetUtils.getTables(customizedConnection.createDataSet());

        assertEquals("table count", tablesBefore.length, tablesAfter.length);

        // Verify tables after
        for (int i = 0; i < tablesAfter.length; i++) {
            ITable tableBefore = tablesBefore[i];
            ITable tableAfter = tablesAfter[i];

            String name = tableAfter.getTableMetaData().getTableName();
            if (name.startsWith("IDENTITY")) {
                assertEquals("row count before: " + name, 0, tableBefore.getRowCount());
                if (dataSet instanceof ForwardOnlyDataSet) {
                    assertTrue(name, tableAfter.getRowCount() > 0);
                } else {
                    Assertion.assertEquals(dataSet.getTable(name), tableAfter);
                }
            } else {
                // Other tables should have not been affected
                Assertion.assertEquals(tableBefore, tableAfter);
            }
        }
    }

    /*
     * test case was added to validate the bug that tables with Identity columns
     * that are not one of the primary keys are able to figure out if an
     * IDENTITY_INSERT is needed. Thanks to Gaetano Di Gregorio for finding the bug.
     */
    @Test
    public void testIdentityInsertNoPK() throws Exception {
        assumeTrue(environmentHasInsertIdentityFeature());
        Reader in = TestUtils.getFileReader("xml/insertIdentityOperationTestNoPK.xml");
        IDataSet xmlDataSet = new FlatXmlDataSetBuilder().build(in);

        ITable[] tablesBefore = DataSetUtils.getTables(customizedConnection.createDataSet());
        InsertIdentityOperation.CLEAN_INSERT.execute(customizedConnection, xmlDataSet);
        ITable[] tablesAfter = DataSetUtils.getTables(customizedConnection.createDataSet());

        // Verify tables after
        for (int i = 0; i < tablesAfter.length; i++) {
            ITable tableBefore = tablesBefore[i];
            ITable tableAfter = tablesAfter[i];

            String name = tableAfter.getTableMetaData().getTableName();
            if (name.equals("TEST_IDENTITY_NOT_PK")) {
                assertEquals("row count before: " + name, 0, tableBefore.getRowCount());
                Assertion.assertEquals(xmlDataSet.getTable(name), tableAfter);
            } else {
                // Other tables should have not been affected
                Assertion.assertEquals(tableBefore, tableAfter);
            }
        }
    }

    @Test
    public void testSetCustomIdentityColumnFilter() throws Exception {
        assumeTrue(environmentHasInsertIdentityFeature());
        customizedConnection.getDatabaseConfig().setIdentityFilter(IDENTITY_FILTER_INVALID);
        try {
            IDataSet dataSet = customizedConnection.createDataSet();
            ITable table = dataSet.getTable("IDENTITY_TABLE");

            InsertIdentityOperation op = new InsertIdentityOperation(DatabaseOperation.INSERT);
            boolean hasIdentityColumn = op.hasIdentityColumn(table.getTableMetaData(), customizedConnection);
            assertFalse("Identity column recognized", hasIdentityColumn);

            // Verify that identity column is still correctly recognized with default
            // identityColumnFilter
            customizedConnection.getDatabaseConfig().setIdentityFilter(null);
            op = new InsertIdentityOperation(DatabaseOperation.INSERT);
            hasIdentityColumn = op.hasIdentityColumn(table.getTableMetaData(), customizedConnection);
            assertTrue("Identity column not recognized", hasIdentityColumn);
        } finally {
            // Reset property
            customizedConnection.getDatabaseConfig().setIdentityFilter(null);
        }
    }

    private static final IColumnFilter IDENTITY_FILTER_INVALID = (tableName, column) -> column.getSqlTypeName()
            .endsWith("invalid");

}
