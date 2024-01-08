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

import org.junit.Test;

/**
 * @author Manuel Laflamme
 * @since Apr 11, 2003
 * @version $Revision$
 */
public class ForwardOnlyTableTest extends DefaultTableTest {

    public ForwardOnlyTableTest() throws Exception {
    }

    @Override
    protected ITable createTable() throws Exception {
        return new ForwardOnlyTable(super.createTable());
    }

    @Override
    public void testGetRowCount() throws Exception {
        try {
            createTable().getRowCount();
            fail("Should have throw UnsupportedOperationException");
        } catch (UnsupportedOperationException e) {

        }
    }

    @Override
    public void testGetValueRowBounds() throws Exception {
        int[] rows = { ROW_COUNT, ROW_COUNT + 1 };
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
    public void testGetValueIterateBackward() throws Exception {
        ITable table = createTable();
        for (int i = 0; i < ROW_COUNT; i++) {
            for (int j = 0; j < COLUMN_COUNT; j++) {
                String columnName = "COLUMN" + j;
                String expected = "row " + i + " col " + j;
                Object value = table.getValue(i, columnName);
                assertEquals("value", expected, value);
            }

            // Try access values from previous row
            for (int j = 0; j < COLUMN_COUNT; j++) {
                String columnName = "COLUMN" + j;
                try {
                    table.getValue(i - 1, columnName);
                } catch (UnsupportedOperationException e) {

                }
            }
        }
    }

    @Test
    public void testGetValueOnEmptyTable() throws Exception {
        MockTableMetaData metaData = new MockTableMetaData("TABLE", new String[] { "C1" });
        ITable table = new ForwardOnlyTable(new DefaultTable(metaData));
        try {
            table.getValue(0, "C1");
            fail("Should have throw RowOutOfBoundsException");
        } catch (RowOutOfBoundsException e) {

        }
    }

}
