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

import java.io.FileReader;
import java.io.Reader;
import java.sql.SQLException;

import org.dbunit.AbstractDatabaseIT;
import org.dbunit.Assertion;
import org.dbunit.TestFeature;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetUtils;
import org.dbunit.dataset.ForwardOnlyDataSet;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.LowerCaseDataSet;
import org.dbunit.dataset.SortedTable;
import org.dbunit.dataset.xml.FlatXmlDataSetBuilder;
import org.dbunit.dataset.xml.XmlDataSet;
import org.dbunit.testutil.TestUtils;
import org.junit.Test;

/**
 * @author Manuel Laflamme
 * @version $Revision$
 * @since Feb 19, 2002
 */
public class InsertOperationIT extends AbstractDatabaseIT {

    public InsertOperationIT() throws Exception {
    }

    @Test
    public void testInsertClob() throws Exception {
        // execute this test only if the target database support CLOB
        if (environment.support(TestFeature.CLOB)) {
            String tableName = "CLOB_TABLE";

            Reader in = new FileReader(TestUtils.getFile("xml/clobInsertTest.xml"));
            IDataSet xmlDataSet = new FlatXmlDataSetBuilder().build(in);

            DatabaseConnection customizedConnection = database.getConnection();
            assertEquals("count before", 0, customizedConnection.getRowCount(tableName));

            DatabaseOperation.INSERT.execute(customizedConnection, xmlDataSet);

            ITable tableAfter = customizedConnection.createDataSet().getTable(tableName);
            assertEquals("count after", 3, tableAfter.getRowCount());
            Assertion.assertEquals(xmlDataSet.getTable(tableName), tableAfter);
        }
    }

    @Test
    public void testInsertBlob() throws Exception {
        // execute this test only if the target database support BLOB
        if (environment.support(TestFeature.BLOB)) {
            String tableName = "BLOB_TABLE";

            Reader in = new FileReader(TestUtils.getFile("xml/blobInsertTest.xml"));
            IDataSet xmlDataSet = new FlatXmlDataSetBuilder().build(in);

            DatabaseConnection customizedConnection = database.getConnection();
            assertEquals("count before", 0, customizedConnection.getRowCount(tableName));

            DatabaseOperation.INSERT.execute(customizedConnection, xmlDataSet);

            ITable tableAfter = customizedConnection.createDataSet().getTable(tableName);
            assertEquals("count after", 3, tableAfter.getRowCount());
            Assertion.assertEquals(xmlDataSet.getTable(tableName), tableAfter);
        }
    }

    @Test
    public void testInsertSdoGeometry() throws Exception {
        // execute this test only if the target database supports SDO_GEOMETRY
        if (environment.support(TestFeature.SDO_GEOMETRY)) {
            String tableName = "SDO_GEOMETRY_TABLE";

            Reader in = new FileReader(TestUtils.getFile("xml/sdoGeometryInsertTest.xml"));
            IDataSet xmlDataSet = new FlatXmlDataSetBuilder().build(in);

            DatabaseConnection customizedConnection = database.getConnection();
            assertEquals("count before", 0, customizedConnection.getRowCount(tableName));

            DatabaseOperation.INSERT.execute(customizedConnection, xmlDataSet);

            ITable tableAfter = customizedConnection.createDataSet().getTable(tableName);
            assertEquals("count after", 1, tableAfter.getRowCount());
            Assertion.assertEquals(xmlDataSet.getTable(tableName), tableAfter);
        }
    }

    @Test
    public void testInsertXmlType() throws Exception {
        // execute this test only if the target database support CLOB
        if (environment.support(TestFeature.XML_TYPE)) {
            String tableName = "XML_TYPE_TABLE";

            Reader in = new FileReader(TestUtils.getFile("xml/xmlTypeInsertTest.xml"));
            IDataSet xmlDataSet = new FlatXmlDataSetBuilder().build(in);

            DatabaseConnection customizedConnection = database.getConnection();
            assertEquals("count before", 0, customizedConnection.getRowCount(tableName));

            DatabaseOperation.INSERT.execute(customizedConnection, xmlDataSet);

            ITable tableAfter = customizedConnection.createDataSet().getTable(tableName);
            assertEquals("count after", 3, tableAfter.getRowCount());
            Assertion.assertEquals(xmlDataSet.getTable(tableName), tableAfter);
        }
    }

    @Test
    public void testMissingColumns() throws Exception {
        Reader in = TestUtils.getFileReader("xml/missingColumnTest.xml");
        IDataSet xmlDataSet = new XmlDataSet(in);

        DatabaseConnection customizedConnection = database.getConnection();
        ITable[] tablesBefore = DataSetUtils.getTables(customizedConnection.createDataSet());
        DatabaseOperation.INSERT.execute(customizedConnection, xmlDataSet);
        ITable[] tablesAfter = DataSetUtils.getTables(customizedConnection.createDataSet());

        // verify tables before
        for (ITable table : tablesBefore) {
            String tableName = table.getTableMetaData().getTableName();
            if (tableName.startsWith("EMPTY")) {
                assertEquals(tableName + " before", 0, table.getRowCount());
            }
        }

        // verify tables after
        for (ITable databaseTable : tablesAfter) {
            String tableName = databaseTable.getTableMetaData().getTableName();

            if (tableName.startsWith("EMPTY")) {
                Column[] columns = databaseTable.getTableMetaData().getColumns();
                ITable xmlTable = xmlDataSet.getTable(tableName);

                // verify row count
                assertEquals("row count", xmlTable.getRowCount(), databaseTable.getRowCount());

                // for each table row
                for (int j = 0; j < databaseTable.getRowCount(); j++) {
                    // verify first column values
                    Object expected = xmlTable.getValue(j, columns[0].getColumnName());
                    Object actual = databaseTable.getValue(j, columns[0].getColumnName());

                    assertEquals(tableName + "." + columns[0].getColumnName(), expected, actual);

                    // all remaining columns should be null except mssql server timestamp column
                    // which is of type binary.
                    for (int k = 1; k < columns.length; k++) {
                        String columnName = columns[k].getColumnName();
                        assertEquals(tableName + "." + columnName, null, databaseTable.getValue(j, columnName));
                    }
                }
            }
        }

    }

    @Test
    public void testExecute() throws Exception {
        Reader in = TestUtils.getFileReader("xml/insertOperationTest.xml");
        IDataSet dataSet = new XmlDataSet(in);

        testExecute(dataSet);
    }

    @Test
    public void testExecuteCaseInsensitive() throws Exception {
        Reader in = TestUtils.getFileReader("xml/insertOperationTest.xml");
        IDataSet dataSet = new XmlDataSet(in);

        testExecute(new LowerCaseDataSet(dataSet));
    }

    @Test
    public void testExecuteForwardOnly() throws Exception {
        Reader in = TestUtils.getFileReader("xml/insertOperationTest.xml");
        IDataSet dataSet = new XmlDataSet(in);

        testExecute(new ForwardOnlyDataSet(dataSet));
    }

    private void testExecute(IDataSet dataSet) throws Exception, SQLException {
        DatabaseConnection customizedConnection = database.getConnection();
        ITable[] tablesBefore = DataSetUtils.getTables(customizedConnection.createDataSet());
        DatabaseOperation.INSERT.execute(customizedConnection, dataSet);
        ITable[] tablesAfter = DataSetUtils.getTables(customizedConnection.createDataSet());

        assertEquals("table count", tablesBefore.length, tablesAfter.length);
        for (ITable table : tablesBefore) {
            String name = table.getTableMetaData().getTableName();

            if (name.startsWith("EMPTY")) {
                assertEquals(name + "before", 0, table.getRowCount());
            }
        }

        for (ITable table : tablesAfter) {
            String name = table.getTableMetaData().getTableName();

            if (name.startsWith("EMPTY")) {
                if (dataSet instanceof ForwardOnlyDataSet) {
                    assertTrue(name, table.getRowCount() > 0);
                } else {
                    SortedTable expectedTable = new SortedTable(dataSet.getTable(name),
                            dataSet.getTable(name).getTableMetaData());
                    SortedTable actualTable = new SortedTable(table);
                    Assertion.assertEquals(expectedTable, actualTable);
                }
            }
        }
    }
}
