/*
 *
 * The DbUnit Database Testing Framework
 * Copyright (C)2002-2008, DbUnit.org
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

import org.dbunit.dataset.datatype.DataType;

import junit.framework.TestCase;

/**
 * @author gommma
 * @version $Revision$
 * @since 2.3.0
 */
public class ColumnsTest extends TestCase {

    public void testGetColumn() throws Exception {
        Column[] columns = { new Column("c0", DataType.UNKNOWN), new Column("c1", DataType.UNKNOWN),
                new Column("c2", DataType.UNKNOWN), new Column("c3", DataType.UNKNOWN),
                new Column("c4", DataType.UNKNOWN), };

        for (int i = 0; i < columns.length; i++) {
            assertEquals("find column same", columns[i], Columns.getColumn("c" + i, columns));
        }
    }

    public void testGetColumnCaseInsensitive() throws Exception {
        Column[] columns = { new Column("c0", DataType.UNKNOWN), new Column("C1", DataType.UNKNOWN),
                new Column("c2", DataType.UNKNOWN), new Column("C3", DataType.UNKNOWN),
                new Column("c4", DataType.UNKNOWN), };

        for (int i = 0; i < columns.length; i++) {
            assertEquals("find column same", columns[i], Columns.getColumn("c" + i, columns));
        }
    }

    public void testGetColumnValidated() throws Exception {
        Column[] columns = { new Column("c0", DataType.UNKNOWN), new Column("C1", DataType.UNKNOWN),
                new Column("c2", DataType.UNKNOWN), };
        for (int i = 0; i < columns.length; i++) {
            assertEquals("find column same", columns[i], Columns.getColumnValidated("c" + i, columns, "TableABC"));
        }
    }

    public void testGetColumnValidatedColumnNotFound() throws Exception {
        Column[] columns = { new Column("c0", DataType.UNKNOWN), new Column("C1", DataType.UNKNOWN),
                new Column("c2", DataType.UNKNOWN), };
        try {
            Columns.getColumnValidated("A1", columns, "TableABC");
            fail("Should not be able to get a validated column that does not exist");
        } catch (NoSuchColumnException expected) {
            assertEquals("TableABC.A1", expected.getMessage());
        }
    }
}
