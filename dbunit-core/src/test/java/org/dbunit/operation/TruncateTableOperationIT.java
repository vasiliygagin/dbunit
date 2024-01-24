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
package org.dbunit.operation;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeTrue;

import org.dbunit.AbstractDatabaseIT;
import org.dbunit.TestFeature;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.dataset.AbstractDataSetTest;
import org.dbunit.dataset.DataSetUtils;
import org.dbunit.dataset.DefaultDataSet;
import org.dbunit.dataset.EmptyTableDataSet;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.LowerCaseDataSet;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Manuel Laflamme
 * @since Apr 13, 2003
 * @version $Revision$
 */
public class TruncateTableOperationIT extends AbstractDatabaseIT {

    public TruncateTableOperationIT() throws Exception {
    }

    private boolean environmentHasTruncateFeature() {
        return environment.support(TestFeature.TRUNCATE_TABLE);
    }

    protected DatabaseOperation getDeleteAllOperation() {
        return new TruncateTableOperation();
    }

    protected String getExpectedStament(String tableName) {
        return "truncate table " + tableName;
    }

    @Before
    public final void setUp1() throws Exception {
        DatabaseConnection customizedConnection = database.getConnection();
        DatabaseOperation.CLEAN_INSERT.execute(customizedConnection, environment.getInitDataSet());
    }

    @Test
    public void testExecute() throws Exception {
        assumeTrue(environmentHasTruncateFeature());
        DatabaseConnection customizedConnection = database.getConnection();
        IDataSet databaseDataSet = customizedConnection.createDataSet();
        IDataSet dataSet = AbstractDataSetTest.removeExtraTestTables(databaseDataSet);

        testExecute(dataSet);
    }

    @Test
    public void testExecuteEmpty() throws Exception {
        assumeTrue(environmentHasTruncateFeature());
        DatabaseConnection customizedConnection = database.getConnection();
        IDataSet databaseDataSet = customizedConnection.createDataSet();
        IDataSet dataSet = AbstractDataSetTest.removeExtraTestTables(databaseDataSet);

        testExecute(new EmptyTableDataSet(dataSet));
    }

    @Test
    public void testExecuteCaseInsentive() throws Exception {
        assumeTrue(environmentHasTruncateFeature());
        DatabaseConnection customizedConnection = database.getConnection();
        IDataSet dataSet = AbstractDataSetTest.removeExtraTestTables(customizedConnection.createDataSet());

        testExecute(new LowerCaseDataSet(dataSet));
    }

    /*
     * The AbstractDataSetTest.removeExtraTestTables() is required when you run on
     * something besides hypersone (like mssql or oracle) to deal with the extra
     * tables that may not have data.
     *
     * Need something like getDefaultTables or something that is totally cross dbms.
     */
    private void testExecute(IDataSet dataSet) throws Exception {
        // dataSet = dataSet);
        DatabaseConnection customizedConnection = database.getConnection();
        ITable[] tablesBefore = DataSetUtils
                .getTables(AbstractDataSetTest.removeExtraTestTables(customizedConnection.createDataSet()));
        getDeleteAllOperation().execute(customizedConnection, dataSet);
        ITable[] tablesAfter = DataSetUtils
                .getTables(AbstractDataSetTest.removeExtraTestTables(customizedConnection.createDataSet()));

        assertTrue("table count > 0", tablesBefore.length > 0);
        assertEquals("table count", tablesBefore.length, tablesAfter.length);
        for (ITable table : tablesBefore) {
            String name = table.getTableMetaData().getTableName();

            if (!name.toUpperCase().startsWith("EMPTY")) {
                assertTrue(name + " before", table.getRowCount() > 0);
            }
        }

        for (int i = 0; i < tablesAfter.length; i++) {
            ITable table = tablesAfter[i];
            String name = table.getTableMetaData().getTableName();
            assertEquals(name + " after " + i, 0, table.getRowCount());
        }
    }

    @Test
    public void testExecuteWithEmptyDataset() throws Exception {
        assumeTrue(environmentHasTruncateFeature());
        DatabaseConnection customizedConnection = database.getConnection();
        getDeleteAllOperation().execute(customizedConnection, new DefaultDataSet(new ITable[0]));
    }
}
