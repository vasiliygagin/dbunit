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
import org.dbunit.Assertion;
import org.dbunit.TestFeature;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.dataset.CompositeDataSet;
import org.dbunit.dataset.ForwardOnlyDataSet;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.LowerCaseDataSet;
import org.dbunit.dataset.NoPrimaryKeyException;
import org.dbunit.dataset.xml.FlatXmlDataSetBuilder;
import org.dbunit.dataset.xml.XmlDataSet;
import org.dbunit.testutil.TestUtils;
import org.junit.Test;

/**
 * @author Manuel Laflamme
 * @version $Revision$
 * @since Feb 19, 2002
 */
public class UpdateOperationIT extends AbstractDatabaseIT {

    ////////////////////////////////////////////////////////////////////////////
    //

    public UpdateOperationIT() throws Exception {
    }

    @Override
    protected IDataSet getDataSet() throws Exception {
        IDataSet dataSet = super.getDataSet();

        if (environment.support(TestFeature.BLOB)) {
            dataSet = new CompositeDataSet(
                    new FlatXmlDataSetBuilder().build(TestUtils.getFile("xml/blobInsertTest.xml")), dataSet);
        }

        if (environment.support(TestFeature.CLOB)) {
            dataSet = new CompositeDataSet(
                    new FlatXmlDataSetBuilder().build(TestUtils.getFile("xml/clobInsertTest.xml")), dataSet);
        }

        if (environment.support(TestFeature.SDO_GEOMETRY)) {
            dataSet = new CompositeDataSet(
                    new FlatXmlDataSetBuilder().build(TestUtils.getFile("xml/sdoGeometryInsertTest.xml")), dataSet);
        }

        if (environment.support(TestFeature.XML_TYPE)) {
            dataSet = new CompositeDataSet(
                    new FlatXmlDataSetBuilder().build(TestUtils.getFile("xml/xmlTypeInsertTest.xml")), dataSet);
        }

        return dataSet;
    }

    @Test
    public void testUpdateClob() throws Exception {
        // execute this test only if the target database support CLOB
        if (environment.support(TestFeature.CLOB)) {
            String tableName = "CLOB_TABLE";
            DatabaseConnection customizedConnection = database.getConnection();

            {
                IDataSet beforeDataSet = new FlatXmlDataSetBuilder().build(TestUtils.getFile("xml/clobInsertTest.xml"));

                ITable tableBefore = customizedConnection.createDataSet().getTable(tableName);
                assertEquals("count before", 3, customizedConnection.getRowCount(tableName));
                Assertion.assertEquals(beforeDataSet.getTable(tableName), tableBefore);
            }

            IDataSet afterDataSet = new FlatXmlDataSetBuilder().build(TestUtils.getFile("xml/clobUpdateTest.xml"));
            DatabaseOperation.REFRESH.execute(customizedConnection, afterDataSet);

            {
                ITable tableAfter = customizedConnection.createDataSet().getTable(tableName);
                assertEquals("count after", 4, tableAfter.getRowCount());
                Assertion.assertEquals(afterDataSet.getTable(tableName), tableAfter);
            }
        }
    }

    @Test
    public void testUpdateBlob() throws Exception {
        // execute this test only if the target database support BLOB
        if (environment.support(TestFeature.BLOB)) {
            String tableName = "BLOB_TABLE";

            DatabaseConnection customizedConnection = database.getConnection();
            {
                IDataSet beforeDataSet = new FlatXmlDataSetBuilder().build(TestUtils.getFile("xml/blobInsertTest.xml"));

                ITable tableBefore = customizedConnection.createDataSet().getTable(tableName);
                assertEquals("count before", 3, customizedConnection.getRowCount(tableName));
                Assertion.assertEquals(beforeDataSet.getTable(tableName), tableBefore);

//                System.out.println("****** BEFORE *******");
//                FlatXmlDataSet.write(_connection.createDataSet(), System.out);
            }

            IDataSet afterDataSet = new FlatXmlDataSetBuilder().build(TestUtils.getFile("xml/blobUpdateTest.xml"));
            DatabaseOperation.REFRESH.execute(customizedConnection, afterDataSet);

            {
                ITable tableAfter = customizedConnection.createDataSet().getTable(tableName);
                assertEquals("count after", 4, tableAfter.getRowCount());
                Assertion.assertEquals(afterDataSet.getTable(tableName), tableAfter);

//                System.out.println("****** AFTER *******");
//                FlatXmlDataSet.write(_connection.createDataSet(), System.out);
            }
        }
    }

    @Test
    public void testUpdateSdoGeometry() throws Exception {
        // execute this test only if the target database supports SDO_GEOMETRY
        if (environment.support(TestFeature.SDO_GEOMETRY)) {
            String tableName = "SDO_GEOMETRY_TABLE";

            DatabaseConnection customizedConnection = database.getConnection();
            {
                IDataSet beforeDataSet = new FlatXmlDataSetBuilder()
                        .build(TestUtils.getFile("xml/sdoGeometryInsertTest.xml"));

                ITable tableBefore = customizedConnection.createDataSet().getTable(tableName);
                assertEquals("count before", 1, customizedConnection.getRowCount(tableName));
                Assertion.assertEquals(beforeDataSet.getTable(tableName), tableBefore);
            }

            IDataSet afterDataSet = new FlatXmlDataSetBuilder()
                    .build(TestUtils.getFile("xml/sdoGeometryUpdateTest.xml"));
            DatabaseOperation.REFRESH.execute(customizedConnection, afterDataSet);

            {
                ITable tableAfter = customizedConnection.createDataSet().getTable(tableName);
                assertEquals("count after", 8, tableAfter.getRowCount());
                Assertion.assertEquals(afterDataSet.getTable(tableName), tableAfter);
            }
        }
    }

    @Test
    public void testUpdateXmlType() throws Exception {
        // execute this test only if the target database support XML_TYPE
        if (environment.support(TestFeature.XML_TYPE)) {
            String tableName = "XML_TYPE_TABLE";

            DatabaseConnection customizedConnection = database.getConnection();
            {
                IDataSet beforeDataSet = new FlatXmlDataSetBuilder()
                        .build(TestUtils.getFile("xml/xmlTypeInsertTest.xml"));

                ITable tableBefore = customizedConnection.createDataSet().getTable(tableName);
                assertEquals("count before", 3, customizedConnection.getRowCount(tableName));
                Assertion.assertEquals(beforeDataSet.getTable(tableName), tableBefore);
            }

            IDataSet afterDataSet = new FlatXmlDataSetBuilder().build(TestUtils.getFile("xml/xmlTypeUpdateTest.xml"));
            DatabaseOperation.REFRESH.execute(customizedConnection, afterDataSet);

            {
                ITable tableAfter = customizedConnection.createDataSet().getTable(tableName);
                assertEquals("count after", 4, tableAfter.getRowCount());
                Assertion.assertEquals(afterDataSet.getTable(tableName), tableAfter);
            }
        }
    }

    @Test
    public void testExecute() throws Exception {
        Reader in = new FileReader(TestUtils.getFile("xml/updateOperationTest.xml"));
        IDataSet dataSet = new XmlDataSet(in);

        testExecute(dataSet);

    }

    @Test
    public void testExecuteCaseInsensitive() throws Exception {
        Reader in = new FileReader(TestUtils.getFile("xml/updateOperationTest.xml"));
        IDataSet dataSet = new XmlDataSet(in);

        testExecute(new LowerCaseDataSet(dataSet));
    }

    @Test
    public void testExecuteForwardOnly() throws Exception {
        Reader in = new FileReader(TestUtils.getFile("xml/updateOperationTest.xml"));
        IDataSet dataSet = new XmlDataSet(in);

        testExecute(new ForwardOnlyDataSet(dataSet));
    }

    @Test
    public void testExecuteAndNoPrimaryKeys() throws Exception {
        String tableName = "TEST_TABLE";

        Reader reader = TestUtils.getFileReader("xml/updateOperationNoPKTest.xml");
        IDataSet dataSet = new FlatXmlDataSetBuilder().build(reader);

        DatabaseConnection customizedConnection = database.getConnection();
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

    private void testExecute(IDataSet dataSet) throws Exception {
        String tableName = "PK_TABLE";
        String[] columnNames = { "PK0", "PK1", "PK2", "NORMAL0", "NORMAL1" };
        int modifiedRow = 1;

        // verify table before
        ITable tableBefore = createOrderedTable(tableName, columnNames[0]);
        assertEquals("row count before", 3, tableBefore.getRowCount());

        DatabaseConnection customizedConnection = database.getConnection();
        DatabaseOperation.UPDATE.execute(customizedConnection, dataSet);

        ITable tableAfter = createOrderedTable(tableName, columnNames[0]);
        assertEquals("row count after", 3, tableAfter.getRowCount());
        for (int i = 0; i < tableAfter.getRowCount(); i++) {
            // verify modified row
            if (i == modifiedRow) {
                assertEquals("PK0", "1", tableAfter.getValue(i, "PK0").toString());
                assertEquals("PK1", "1", tableAfter.getValue(i, "PK1").toString());
                assertEquals("PK2", "1", tableAfter.getValue(i, "PK2").toString());
                assertEquals("NORMAL0", "toto", tableAfter.getValue(i, "NORMAL0").toString());
                assertEquals("NORMAL1", "qwerty", tableAfter.getValue(i, "NORMAL1").toString());
            }
            // all other row must be equals than before update
            else {
                for (int j = 0; j < columnNames.length; j++) {
                    String name = columnNames[j];
                    Object valueAfter = tableAfter.getValue(i, name);
                    Object valueBefore = tableBefore.getValue(i, name);
                    assertEquals("c=" + name + ",r=" + j, valueBefore, valueAfter);
                }
            }
        }
    }

}
