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

package org.dbunit.dataset.csv;

import java.io.File;

import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITable;
import org.dbunit.testutil.TestUtils;

import junit.framework.TestCase;

/**
 * @author Lenny Marks (lenny@aps.org)
 * @version $Revision$
 * @since Sep 12, 2004 (pre 2.3)
 */
public class CsvDataSetTest extends TestCase {
    protected static final File DATASET_DIR = TestUtils.getFile("csv/orders");

    public CsvDataSetTest(String s) {
        super(s);
    }

    public void testNullColumns() throws DataSetException {
        File csvDir = DATASET_DIR;

        CsvDataSet dataSet = new CsvDataSet(csvDir);

        ITable table = dataSet.getTable("orders");

        assertNull(table.getValue(4, "description"));

    }
}
