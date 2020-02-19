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

package org.dbunit.dataset.yaml;

import org.dbunit.dataset.AbstractTableTest;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.testutil.TestUtils;

import java.io.FileInputStream;
import java.io.InputStream;

/**
 * @author Björn Beskow
 * @version $Revision$ $Date$
 */
public class YmlTableTest extends AbstractTableTest
{
    public YmlTableTest(String s)
    {
        super(s);
    }

    protected ITable createTable() throws Exception
    {
        return createDataSet().getTable("TEST_TABLE");
    }

    protected IDataSet createDataSet() throws Exception
    {
        InputStream in = new FileInputStream(TestUtils.getFile("yaml/yamlTableTest.yml"));
        return new YamlDataSet(in);
    }

    public void testGetMissingValue() throws Exception
    {
        Object[][] expected = {
            {"row 0 col 0", null, null},
            {null, "row 1 col 1", null},
            {null, null, "row 2 col 2"}
        };

        ITable table = createDataSet().getTable("MISSING_VALUES");

        Column[] columns = table.getTableMetaData().getColumns();
        assertEquals("column count", expected[0].length, columns.length);
        assertEquals("row count", expected.length, table.getRowCount());
        for (int row = 0; row < table.getRowCount(); row++)
        {
            for (int col = 0; col < columns.length; col++)
            {
                assertEquals("value " + row + ":" + col, expected[row][col],
                table.getValue(row, columns[col].getColumnName()));
            }
        }
    }
}
