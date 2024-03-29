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
import static org.junit.Assert.fail;

import java.io.Reader;

import org.dbunit.AbstractDatabaseIT;
import org.dbunit.Assertion;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.dataset.ForwardOnlyDataSet;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.LowerCaseDataSet;
import org.dbunit.dataset.NoPrimaryKeyException;
import org.dbunit.dataset.xml.FlatXmlDataSetBuilder;
import org.dbunit.testutil.TestUtils;
import org.junit.Test;

/**
 * @author Manuel Laflamme
 * @version $Revision$
 * @since Feb 19, 2002
 */
public class RefreshOperationIT extends AbstractDatabaseIT {

    public RefreshOperationIT() throws Exception {
    }

    @Test
    public void testExecute() throws Exception {
        Reader reader = TestUtils.getFileReader("xml/refreshOperationTest.xml");
        IDataSet dataSet = new FlatXmlDataSetBuilder().build(reader);

        testExecute(dataSet);
    }

    @Test
    public void testExecuteCaseInsensitive() throws Exception {
        Reader reader = TestUtils.getFileReader("xml/refreshOperationTest.xml");
        IDataSet dataSet = new FlatXmlDataSetBuilder().build(reader);

        testExecute(new LowerCaseDataSet(dataSet));
    }

    @Test
    public void testExecuteForwardOnly() throws Exception {
        Reader reader = TestUtils.getFileReader("xml/refreshOperationTest.xml");
        IDataSet dataSet = new FlatXmlDataSetBuilder().build(reader);

        testExecute(new ForwardOnlyDataSet(dataSet));
    }

    private void testExecute(IDataSet dataSet) throws Exception {
        String[] tableNames = { "PK_TABLE", "ONLY_PK_TABLE" };
        int[] tableRowCount = { 3, 1 };
        String primaryKey = "PK0";

        // verify table before
        assertEquals("array lenght", tableNames.length, tableRowCount.length);
        for (int i = 0; i < tableNames.length; i++) {
            ITable tableBefore = createOrderedTable(tableNames[i], primaryKey);
            assertEquals("row count before", tableRowCount[i], tableBefore.getRowCount());
        }

        DatabaseConnection customizedConnection = database.getConnection();
        DatabaseOperation.REFRESH.execute(customizedConnection, dataSet);

        // verify table after
        IDataSet expectedDataSet = new FlatXmlDataSetBuilder()
                .build(TestUtils.getFileReader("xml/refreshOperationTestExpected.xml"));

        for (String tableName : tableNames) {
            ITable expectedTable = expectedDataSet.getTable(tableName);
            ITable tableAfter = createOrderedTable(tableName, primaryKey);
            Assertion.assertEquals(expectedTable, tableAfter);
        }
    }

    @Test
    public void testExecuteAndNoPrimaryKeys() throws Exception {
        DatabaseConnection customizedConnection = database.getConnection();
        String tableName = "TEST_TABLE";

        Reader reader = TestUtils.getFileReader("xml/refreshOperationNoPKTest.xml");
        IDataSet dataSet = new FlatXmlDataSetBuilder().build(reader);

        // verify table before
        assertEquals("row count before", 6, customizedConnection.getRowCount(tableName));

        try {
            DatabaseOperation.REFRESH.execute(customizedConnection, dataSet);
            fail("Should not be here!");
        } catch (NoPrimaryKeyException e) {

        }

        // verify table after
        assertEquals("row count before", 6, customizedConnection.getRowCount(tableName));
    }
}
