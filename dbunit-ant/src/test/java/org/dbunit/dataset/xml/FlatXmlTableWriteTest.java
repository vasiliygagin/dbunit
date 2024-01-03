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

package org.dbunit.dataset.xml;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.Writer;

import org.dbunit.Assertion;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.CompositeTable;
import org.dbunit.dataset.DefaultDataSet;
import org.dbunit.dataset.DefaultTableMetaData;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.ITableMetaData;
import org.dbunit.dataset.NoSuchColumnException;
import org.dbunit.dataset.RowOutOfBoundsException;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Manuel Laflamme
 * @version $Revision$
 * @since Mar 13, 2002
 */
public class FlatXmlTableWriteTest {

    protected IDataSet createDataSet() throws Exception {
        File tempFile = File.createTempFile("flatXmlTableWriteTest", ".xml");
        Writer out = new FileWriter(tempFile);
        try {
            // write DefaultTable in temp file
            try {
                IDataSet dataSet = createDataSet(true);

                FlatXmlWriter datasetWriter = new FlatXmlWriter(out, null);
                datasetWriter.setIncludeEmptyTable(true);
                datasetWriter.write(dataSet);
            } finally {
                out.close();
            }

            // load new dataset from temp file
            try (FileReader in = new FileReader(tempFile)) {
                return new FlatXmlDataSetBuilder().build(in);
            }
        } finally {
            tempFile.delete();
        }
    }

    @Test
    public void testWriteMultipleTable() throws Exception {
        int tableCount = 5;
        ITable sourceTable = createTable();

        ITable[] tables = new ITable[tableCount];
        for (int i = 0; i < tables.length; i++) {
            ITableMetaData metaData = new DefaultTableMetaData("table" + i,
                    sourceTable.getTableMetaData().getColumns());
            tables[i] = new CompositeTable(metaData, sourceTable);
        }

        IDataSet dataSet = new DefaultDataSet(tables);
        File tempFile = File.createTempFile("flatXmlTableWriteTest", "xml");
        Writer out = new FileWriter(tempFile);
        try {
            // write DefaultTable in temp file
            try {
                FlatXmlWriter datasetWriter = new FlatXmlWriter(out, null);
                datasetWriter.setIncludeEmptyTable(true);
                datasetWriter.write(dataSet);
            } finally {
                out.close();
            }

            // load new dataset from temp file
            try (FileReader in = new FileReader(tempFile)) {
                FlatXmlDataSet xmlDataSet2 = new FlatXmlDataSetBuilder().build(in);

                // verify each table
                for (int i = 0; i < tables.length; i++) {
                    ITable table = tables[i];
                    Assertion.assertEquals(table, xmlDataSet2.getTable(xmlDataSet2.getTableNames()[i]));
                }
            }
        } finally {
            tempFile.delete();
        }
    }

    private ITable createTable() throws Exception {
        return createDataSet(true).getTable("TEST_TABLE");
    }

    private IDataSet createDataSet(boolean noneAsNull) throws Exception {
        return new FlatXmlDataSetBuilder().build(new File("src/test/resources/xml/flatXmlTableTest.xml"));
    }

    @Test
    public void testGetMissingValue() throws Exception {
        int row = 0;
        Object[] expected = { "row 1 col 0", null, "row 1 col 2" };

        ITable table = createDataSet(false).getTable("MISSING_VALUES");

        Column[] columns = table.getTableMetaData().getColumns();
        assertEquals("column count", expected.length, columns.length);
        assertEquals("row count", 1, table.getRowCount());
        for (int i = 0; i < columns.length; i++) {
            assertEquals("value " + i, expected[i], table.getValue(row, columns[i].getColumnName()));
        }
    }

    public void testLoadCRLF() throws Exception {
        int row = 0;
        Object[] expected = { "row 0 \n col 0 \r" }; // in the expected result the &#xA; and &#xD; should be replaced by
                                                     // \n and \r

        ITable table = createDataSet(false).getTable("TABLE_VALUE_METACHARS");

        Column[] columns = table.getTableMetaData().getColumns();
        assertEquals("column count", expected.length, columns.length);
        assertEquals("row count", 1, table.getRowCount());
        for (int i = 0; i < columns.length; i++) {
            assertEquals("value " + i, expected[i], table.getValue(row, columns[i].getColumnName()));
        }
    }

    protected static final int ROW_COUNT = 6;
    protected static final int COLUMN_COUNT = 4;

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    /**
     * Returns the string converted as an identifier according to the metadata rules
     * of the database environment. Most databases convert all metadata identifiers
     * to uppercase. PostgreSQL converts identifiers to lowercase. MySQL preserves
     * case.
     *
     * @param str The identifier.
     * @return The identifier converted according to database rules.
     */
    protected String convertString(String str) throws Exception {
        return str;
    }

    ////////////////////////////////////////////////////////////////////////////
    // Test methods

    public void testGetRowCount() throws Exception {
        assertEquals("row count", ROW_COUNT, createTable().getRowCount());
    }

    public void testTableMetaData() throws Exception {
        Column[] columns = createTable().getTableMetaData().getColumns();
        assertEquals("column count", COLUMN_COUNT, columns.length);
        for (int i = 0; i < columns.length; i++) {
            String expected = convertString("COLUMN" + i);
            String actual = columns[i].getColumnName();
            assertEquals("column name", expected, actual);
        }
    }

    public void testGetValue() throws Exception {
        ITable table = createTable();
        for (int i = 0; i < ROW_COUNT; i++) {
            for (int j = 0; j < COLUMN_COUNT; j++) {
                String columnName = "COLUMN" + j;
                String expected = "row " + i + " col " + j;
                Object value = table.getValue(i, columnName);
                assertEquals("value", expected, value);
            }
        }
    }

    public void testGetValueCaseInsensitive() throws Exception {
        ITable table = createTable();
        for (int i = 0; i < ROW_COUNT; i++) {
            for (int j = 0; j < COLUMN_COUNT; j++) {
                String columnName = "CoLUmN" + j;
                String expected = "row " + i + " col " + j;
                Object value = table.getValue(i, columnName);
                assertEquals("value", expected, value);
            }
        }
    }

    public void testGetValueRowBounds() throws Exception {
        int[] rows = { -2, -1, -ROW_COUNT, ROW_COUNT, ROW_COUNT + 1 };
        ITable table = createTable();
        String columnName = table.getTableMetaData().getColumns()[0].getColumnName();

        for (int row : rows) {
            try {
                table.getValue(row, columnName);
                fail("Should throw a RowOutOfBoundsException!");
            } catch (RowOutOfBoundsException e) {
            }
        }
    }

    public void testGetValueAndNoSuchColumn() throws Exception {
        ITable table = createTable();
        String columnName = "Unknown";

        try {
            table.getValue(0, columnName);
            fail("Should throw a NoSuchColumnException!");
        } catch (NoSuchColumnException e) {
        }
    }

    /**
     * This method is used so sub-classes can disable the tests according to some
     * characteristics of the environment
     *
     * @param testName name of the test to be checked
     * @return flag indicating if the test should be executed or not
     */
    protected boolean runTest(String testName) {
        return true;
    }
}
