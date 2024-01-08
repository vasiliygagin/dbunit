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

package org.dbunit.database;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

import org.dbunit.DatabaseEnvironment;
import org.dbunit.DatabaseEnvironmentLoader;
import org.dbunit.DatabaseUnitRuntimeException;
import org.dbunit.TestFeature;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.NoSuchColumnException;
import org.dbunit.dataset.RowOutOfBoundsException;
import org.dbunit.operation.DatabaseOperation;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Manuel Laflamme
 * @version $Revision$
 * @since Feb 19, 2002
 */
public class ScrollableResultSetTableTest {

    private DatabaseEnvironment environment;

    public ScrollableResultSetTableTest() throws Exception {
        environment = DatabaseEnvironmentLoader.getInstance();
    }

    protected ITable createTable() throws Exception {
        IDatabaseConnection connection = environment.getConnection();

        DatabaseOperation.CLEAN_INSERT.execute(connection, environment.getInitDataSet());

        String selectStatement = "select * from TEST_TABLE order by COLUMN0";
        return new ScrollableResultSetTable("TEST_TABLE", selectStatement, connection);
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

    private boolean hasScrollableResultFeature() {
        try {
            final boolean runIt = environment.support(TestFeature.SCROLLABLE_RESULTSET);
            return runIt;
        } catch (Exception e) {
            throw new DatabaseUnitRuntimeException(e);
        }
    }

    ////////////////////////////////////////////////////////////////////////////
    // Test methods

    @Test
    public void testGetRowCount() throws Exception {
        assumeTrue(hasScrollableResultFeature());
        assertEquals("row count", ROW_COUNT, createTable().getRowCount());
    }

    @Test
    public void testTableMetaData() throws Exception {
        assumeTrue(hasScrollableResultFeature());
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
        assumeTrue(hasScrollableResultFeature());
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
        assumeTrue(hasScrollableResultFeature());
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
        assumeTrue(hasScrollableResultFeature());
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
        assumeTrue(hasScrollableResultFeature());
        ITable table = createTable();
        String columnName = "Unknown";

        try {
            table.getValue(0, columnName);
            fail("Should throw a NoSuchColumnException!");
        } catch (NoSuchColumnException e) {
        }
    }
}
