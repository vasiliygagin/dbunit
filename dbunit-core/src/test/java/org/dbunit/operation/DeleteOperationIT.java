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

import java.io.FileReader;
import java.io.Reader;

import org.dbunit.AbstractDatabaseIT;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.ITableMetaData;
import org.dbunit.dataset.LowerCaseDataSet;
import org.dbunit.dataset.NoPrimaryKeyException;
import org.dbunit.dataset.xml.XmlDataSet;
import org.dbunit.testutil.TestUtils;
import org.junit.Test;

/**
 * @author Manuel Laflamme
 * @version $Revision$
 * @since Feb 19, 2002
 */
public class DeleteOperationIT extends AbstractDatabaseIT {

    public DeleteOperationIT() throws Exception {
    }

    @Override
    protected IDataSet getDataSet() throws Exception {
        return super.getDataSet();
    }

    @Test
    public void testExecuteAndNoPrimaryKey() throws Exception {
        DatabaseConnection customizedConnection = database.getConnection();
        IDataSet dataSet = customizedConnection.createDataSet();
        ITableMetaData metaData = dataSet.getTableMetaData("TEST_TABLE");
        try {
            new DeleteOperation().getOperationData(metaData, null, customizedConnection);
            fail("Should throw a NoPrimaryKeyException");
        } catch (NoPrimaryKeyException e) {
        }
    }

    @Test
    public void testExecute() throws Exception {
        Reader in = new FileReader(TestUtils.getFile("xml/deleteOperationTest.xml"));
        IDataSet dataSet = new XmlDataSet(in);

        testExecute(dataSet);

    }

    @Test
    public void testExecuteCaseInsensitive() throws Exception {
        Reader in = new FileReader(TestUtils.getFile("xml/deleteOperationTest.xml"));
        IDataSet dataSet = new XmlDataSet(in);

        testExecute(new LowerCaseDataSet(dataSet));
    }

    private void testExecute(IDataSet dataSet) throws Exception {
        String tableName = "PK_TABLE";
        String columnName = "PK0";

        // verify table before
        ITable tableBefore = createOrderedTable(tableName, columnName);
        assertEquals("row count before", 3, tableBefore.getRowCount());
        assertEquals("before", "0", tableBefore.getValue(0, columnName).toString());
        assertEquals("before", "1", tableBefore.getValue(1, columnName).toString());
        assertEquals("before", "2", tableBefore.getValue(2, columnName).toString());

        DatabaseConnection customizedConnection = database.getConnection();
        DatabaseOperation.DELETE.execute(customizedConnection, dataSet);

        ITable tableAfter = createOrderedTable(tableName, columnName);
        assertEquals("row count after", 2, tableAfter.getRowCount());
        assertEquals("after", "0", tableAfter.getValue(0, columnName).toString());
        assertEquals("after", "2", tableAfter.getValue(1, columnName).toString());
    }
}
