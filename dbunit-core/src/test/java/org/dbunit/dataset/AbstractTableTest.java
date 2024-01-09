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

package org.dbunit.dataset;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.dbunit.AbstractDatabaseTest;
import org.junit.Test;

/**
 * @author Manuel Laflamme
 * @version $Revision$
 * @since Feb 17, 2002
 */
public abstract class AbstractTableTest extends AbstractDatabaseTest {

    protected static final int ROW_COUNT = 6;
    protected static final int COLUMN_COUNT = 4;

    public AbstractTableTest() throws Exception {
    }

    /**
     * Creates a table having 6 row and 4 column where columns are named "COLUMN1,
     * COLUMN2, COLUMN3, COLUMN4" and values are string follwing this template "row
     * ? col ?"
     */
    protected abstract ITable createTable() throws Exception;

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
    public abstract void testGetMissingValue() throws Exception;

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
