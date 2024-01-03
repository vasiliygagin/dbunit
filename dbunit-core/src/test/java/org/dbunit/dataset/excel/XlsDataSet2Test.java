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

import java.io.File;
import java.io.FileInputStream;

import org.dbunit.dataset.AbstractDataSetTest;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.Columns;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;

/**
 * @author Manuel Laflamme
 * @since Feb 22, 2003
 * @version $Revision$
 */
public class XlsDataSet2Test extends AbstractDataSetTest {
    public XlsDataSet2Test(String s) {
        super(s);
    }

    @Override
    protected IDataSet createDataSet() throws Exception {
        return new XlsDataSet2(new File("src/test/resources/xml/dataSetTest.xls"));
    }

    @Override
    protected IDataSet createDuplicateDataSet() throws Exception {
        return new XlsDataSet2(new File("src/test/resources/xml/dataSetDuplicateTest.xls"));
    }

    @Override
    protected IDataSet createMultipleCaseDuplicateDataSet() throws Exception {
        throw new UnsupportedOperationException(
                "Excel does not support the same sheet name with different cases in one file");
    }

    @Override
    public void testCreateMultipleCaseDuplicateDataSet() throws Exception {
        // Not supported
    }

    public void testColumnNameWithSpace() throws Exception {
        IDataSet dataSet = new XlsDataSet2(new FileInputStream("src/test/resources/xml/contactor.xls"));
        ITable customerTable = dataSet.getTable("customer");
        Column column = Columns.getColumn("name", customerTable.getTableMetaData().getColumns());
        assertNotNull(column);
    }

}
