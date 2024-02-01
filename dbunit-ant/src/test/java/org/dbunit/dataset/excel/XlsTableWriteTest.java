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
package org.dbunit.dataset.excel;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.util.TimeZone;

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
import org.dbunit.dataset.datatype.DataType;
import org.junit.Test;

public class XlsTableWriteTest {

    protected IDataSet createDataSet() throws Exception {
        File tempFile = File.createTempFile("tableWriteTest", ".xls");
//        System.out.println(tempFile.getAbsoluteFile());
        OutputStream out = new FileOutputStream(tempFile);
        try {
            // write source dataset in temp file
            try {
                new XlsDataSetWriter().write(super_createDataSet(), out);
            } finally {
                out.close();
            }

            // load new dataset from temp file
            try (InputStream in = new FileInputStream(tempFile)) {
                return new XlsDataSet(in);
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
        File tempFile = File.createTempFile("tableWriteTest", ".xls");
        OutputStream out = new FileOutputStream(tempFile);
        try {
            // write DefaultTable in temp file
            try {
                new XlsDataSetWriter().write(dataSet, out);
            } finally {
                out.close();
            }

            // load new dataset from temp file
            try (FileInputStream in = new FileInputStream(tempFile)) {
                XlsDataSet dataSet2 = new XlsDataSet(in);

                // verify each table
                for (int i = 0; i < tables.length; i++) {
                    ITable table = tables[i];
                    Assertion.assertEquals(table, dataSet2.getTable(dataSet2.getTableNames()[i]));
                }
            }

        } finally {
            tempFile.delete();
        }
    }

    private ITable createTable() throws Exception {
        return createDataSet().getTable("TEST_TABLE");
    }

    private IDataSet super_createDataSet() throws Exception {
        return new XlsDataSet(new File("src/test/resources/xml/tableTest.xls").getAbsoluteFile());
    }

    @Test
    public void testGetMissingValue() throws Exception {
        int row = 0;
        Object[] expected = { "row 0 col 0", null, "row 0 col 2" };

        ITable table = createDataSet().getTable("MISSING_VALUES");

        Column[] columns = table.getTableMetaData().getColumns();
        assertEquals("column count", expected.length, columns.length);
        assertEquals("row count", 1, table.getRowCount());
        for (int i = 0; i < columns.length; i++) {
            assertEquals("value " + i, expected[i], table.getValue(row, columns[i].getColumnName()));
        }
    }

    @Test
    public void testEmptyTableColumns() throws Exception {
        Column[] expectedColumns = { new Column("COLUMN0", DataType.UNKNOWN), new Column("COLUMN1", DataType.UNKNOWN),
                new Column("COLUMN2", DataType.UNKNOWN), new Column("COLUMN3", DataType.UNKNOWN) };
        ITable table = createDataSet().getTable("EMPTY_TABLE");

        Column[] columns = table.getTableMetaData().getColumns();
        assertEquals("Column count", expectedColumns.length, columns.length);
        for (int i = 0; i < columns.length; i++) {
            assertEquals("Column " + i, expectedColumns[i], columns[i]);
        }
    }

    @Test
    public void testEmptySheet() throws Exception {
        ITable table = createDataSet().getTable("EMPTY_SHEET");

        Column[] columns = table.getTableMetaData().getColumns();
        assertEquals("Column count", 0, columns.length);
    }

    @Test
    public void testDifferentDatatypes() throws Exception {
        int row = 0;
        ITable table = createDataSet().getTable("TABLE_DIFFERENT_DATATYPES");

        // When cell type is numeric and cell value is datetime,
        // Apache-POI returns datetime with system default timezone offset.
        // And java.util.Date#getTime() returns time without timezone offset (= UTC).
        // So actual time values in this case will be UTC time in the system default
        // timezone.
        // Expected time values also should be UTC time in the system default timezone.
        long tzOffset = TimeZone.getDefault().getRawOffset();
        Object[] expected = {
//              new Date(0-tzOffset),
//              new Date(0-tzOffset + (10*ONE_HOUR_IN_MILLIS + 45*ONE_MINUTE_IN_MILLIS)),
//              new Date(0-tzOffset + (13*ONE_HOUR_IN_MILLIS + 30*ONE_MINUTE_IN_MILLIS + 55*ONE_SECOND_IN_MILLIS) ),
//              new Long(25569),// Dates stored as Long numbers
//              new Long(25569447916666668L),
//              new Long(563136574074074L),
                new Long(0 - tzOffset), // Dates stored as Long numbers
                new Long(38700000 - tzOffset), new Long(-2209026545000L - tzOffset), new BigDecimal("10000.00"),
                new BigDecimal("-200"), new BigDecimal("12345.123456789000"), new Long(1233398764000L - tzOffset),
                new Long(1233332866000L) // The last column is a dbunit-date-formatted column in the excel sheet
        };

        Column[] columns = table.getTableMetaData().getColumns();
        assertEquals("column count", expected.length, columns.length);
        for (int i = 0; i < columns.length; i++) {
            Object actual = table.getValue(row, columns[i].getColumnName());
            String typesResult = " expected=" + (expected[i] != null ? expected[i].getClass().getName() : "null")
                    + " - actual=" + (actual != null ? actual.getClass().getName() : "null");
            assertEquals("value " + i + " (" + typesResult + ")", expected[i], actual);
        }
    }

    @Test
    public void testNumberAsText() throws Exception {
        int row = 0;
        ITable table = createDataSet().getTable("TABLE_NUMBER_AS_TEXT");

        String[] expected = { "0", "666", "66.6", "66.6", "-6.66" };

        Column[] columns = table.getTableMetaData().getColumns();
        assertEquals("column count", expected.length, columns.length);
        for (int i = 0; i < columns.length; i++) {
            String columnName = columns[i].getColumnName();
            Object actual = table.getValue(row, columnName).toString();
            assertEquals(columns[i].getColumnName(), expected[i], actual);
        }
    }

    protected static final int ROW_COUNT = 6;
    protected static final int COLUMN_COUNT = 4;

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

    @Test
    public void testGetRowCount() throws Exception {
        assertEquals("row count", ROW_COUNT, createTable().getRowCount());
    }

    @Test
    public void testTableMetaData() throws Exception {
        Column[] columns = createTable().getTableMetaData().getColumns();
        assertEquals("column count", COLUMN_COUNT, columns.length);
        for (int i = 0; i < columns.length; i++) {
            String expected = convertString("COLUMN" + i);
            String actual = columns[i].getColumnName();
            assertEquals("column name", expected, actual);
        }
    }

    @Test
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

    @Test
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

    @Test
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

    @Test
    public void testGetValueAndNoSuchColumn() throws Exception {
        ITable table = createTable();
        String columnName = "Unknown";

        try {
            table.getValue(0, columnName);
            fail("Should throw a NoSuchColumnException!");
        } catch (NoSuchColumnException e) {
        }
    }
}
