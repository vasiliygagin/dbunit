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
package org.dbunit.dataset.filter;

import static org.junit.Assert.assertEquals;

import org.dbunit.dataset.AbstractTest;
import org.dbunit.dataset.CompositeDataSet;
import org.dbunit.dataset.DefaultDataSet;
import org.dbunit.dataset.DefaultTable;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.xml.XmlDataSet;
import org.dbunit.testutil.TestUtils;
import org.junit.Test;

/**
 * @author Manuel Laflamme
 * @since Mar 9, 2003
 * @version $Revision$
 */
public abstract class AbstractTableFilterTest extends AbstractTest {

    public AbstractTableFilterTest() throws Exception {
    }

    @Override
    protected String[] getExpectedNames() throws Exception {
        return new String[] { "PARENT", "TEST_TABLE", "SECOND_TABLE", "EMPTY_TABLE", "PK_TABLE", "ONLY_PK_TABLE",
                "EMPTY_MULTITYPE_TABLE", };
    }

    @Override
    protected String[] getExpectedLowerNames() throws Exception {
        String[] names = getExpectedNames();
        for (int i = 0; i < names.length; i++) {
            names[i] = names[i].toLowerCase();
        }

        return names;
    }

    protected IDataSet createDataSet() throws Exception {
        IDataSet dataSet1 = new XmlDataSet(TestUtils.getFileReader("xml/dataSetTest.xml"));
        IDataSet dataSet2 = new DefaultDataSet(new DefaultTable(getExtraTableName()));

        IDataSet dataSet = new CompositeDataSet(dataSet1, dataSet2);
        assertEquals("count before filter", getExpectedNames().length + 1, dataSet.getTableNames().length);
        return dataSet;
    }

    protected IDataSet createDuplicateDataSet() throws Exception {
        IDataSet dataSet1 = new XmlDataSet(TestUtils.getFileReader("xml/xmlDataSetDuplicateTest.xml"));
        IDataSet dataSet2 = new DefaultDataSet(new DefaultTable(getExtraTableName()));

        IDataSet dataSet = new CompositeDataSet(dataSet1, dataSet2, false);
        assertEquals("count before filter", getExpectedDuplicateNames().length + 1, dataSet.getTableNames().length);
        return dataSet;
    }

    @Test
    public abstract void testAccept() throws Exception;

    @Test
    public abstract void testIsCaseInsensitiveValidName() throws Exception;

    @Test
    public abstract void testIsValidNameAndInvalid() throws Exception;

    @Test
    public abstract void testGetTableNames() throws Exception;

    @Test
    public abstract void testGetCaseInsensitiveTableNames() throws Exception;

    @Test
    public abstract void testGetReverseTableNames() throws Exception;

    @Test
    public abstract void testGetTableNamesAndTableNotInDecoratedDataSet() throws Exception;

    @Test
    public abstract void testIterator() throws Exception;

    @Test
    public abstract void testCaseInsensitiveIterator() throws Exception;

    @Test
    public abstract void testReverseIterator() throws Exception;

    @Test
    public abstract void testIteratorAndTableNotInDecoratedDataSet() throws Exception;
}
