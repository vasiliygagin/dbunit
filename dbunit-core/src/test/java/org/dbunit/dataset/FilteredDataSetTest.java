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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.FileReader;

import org.dbunit.dataset.filter.SequenceTableFilter;
import org.dbunit.dataset.xml.FlatXmlDataSetBuilder;
import org.dbunit.dataset.xml.XmlDataSet;
import org.dbunit.testutil.TestUtils;
import org.junit.Test;

/**
 * @author Manuel Laflamme
 * @version $Revision$
 * @since Feb 22, 2002
 */
public class FilteredDataSetTest extends AbstractDataSetTest {

    public FilteredDataSetTest() throws Exception {
    }

    @Override
    protected IDataSet createDataSet() throws Exception {
        IDataSet dataSet1 = new XmlDataSet(TestUtils.getFileReader("xml/dataSetTest.xml"));
        IDataSet dataSet2 = new XmlDataSet(TestUtils.getFileReader("xml/filteredDataSetTest.xml"));

        IDataSet dataSet = new CompositeDataSet(dataSet1, dataSet2);
        assertEquals("count before filter", getExpectedNames().length + 1, dataSet.getTableNames().length);
        SequenceTableFilter filter = new SequenceTableFilter(getExpectedNames(), dataSet.isCaseSensitiveTableNames());
        return new FilteredDataSet(filter, dataSet);
    }

    @Override
    protected IDataSet createDuplicateDataSet() throws Exception {
        IDataSet dataSet1 = new XmlDataSet(TestUtils.getFileReader("xml/xmlDataSetDuplicateTest.xml"));
        IDataSet dataSet2 = new XmlDataSet(TestUtils.getFileReader("xml/filteredDataSetTest.xml"));

        assertEquals(2, dataSet1.getTableNames().length);
        assertEquals(1, dataSet2.getTableNames().length);

        IDataSet dataSet = new CompositeDataSet(dataSet1, dataSet2, false);
        assertEquals("count before filter", 3, dataSet.getTableNames().length);
        SequenceTableFilter filter = new SequenceTableFilter(getExpectedDuplicateNames(),
                dataSet.isCaseSensitiveTableNames());
        return new FilteredDataSet(filter, dataSet);
    }

    @Override
    protected IDataSet createMultipleCaseDuplicateDataSet() throws Exception {
        String[] names = getExpectedDuplicateNames();
        names[0] = names[0].toLowerCase();
        IDataSet dataSet = createDuplicateDataSet();

        SequenceTableFilter filter = new SequenceTableFilter(names, dataSet.isCaseSensitiveTableNames());
        return new FilteredDataSet(filter, dataSet);
    }

    @Test
    public void testGetFilteredTableNames() throws Exception {
        String[] originalNames = getExpectedNames();
        String expectedName = originalNames[0];
        IDataSet dataSet = createDataSet();
        assertTrue("original count", dataSet.getTableNames().length > 1);
        SequenceTableFilter filter = new SequenceTableFilter(new String[] { expectedName },
                dataSet.isCaseSensitiveTableNames());

        IDataSet filteredDataSet = new FilteredDataSet(filter, dataSet);
        assertEquals("filtered count", 1, filteredDataSet.getTableNames().length);
        assertEquals("filtered names", expectedName, filteredDataSet.getTableNames()[0]);
    }

    @Test
    public void testGetFilteredTable() throws Exception {
        String[] originalNames = getExpectedNames();
        IDataSet dataSet = createDataSet();
        SequenceTableFilter filter = new SequenceTableFilter(new String[] { originalNames[0] },
                dataSet.isCaseSensitiveTableNames());
        IDataSet filteredDataSet = new FilteredDataSet(filter, dataSet);

        for (int i = 0; i < originalNames.length; i++) {
            String name = originalNames[i];
            if (i == 0) {
                assertEquals("table " + i, name, filteredDataSet.getTable(name).getTableMetaData().getTableName());
            } else {
                try {
                    filteredDataSet.getTable(name);
                    fail("Should throw a NoSuchTableException");
                } catch (NoSuchTableException e) {
                }
            }
        }
    }

    @Test
    public void testGetFilteredTableMetaData() throws Exception {
        String[] originalNames = getExpectedNames();
        IDataSet dataSet = createDataSet();
        SequenceTableFilter filter = new SequenceTableFilter(new String[] { originalNames[0] },
                dataSet.isCaseSensitiveTableNames());
        IDataSet filteredDataSet = new FilteredDataSet(filter, dataSet);

        for (int i = 0; i < originalNames.length; i++) {
            String name = originalNames[i];
            if (i == 0) {
                assertEquals("table " + i, name, filteredDataSet.getTableMetaData(name).getTableName());
            } else {
                try {
                    filteredDataSet.getTableMetaData(name);
                    fail("Should throw a NoSuchTableException");
                } catch (NoSuchTableException e) {
                }
            }
        }
    }

    @Test
    public void testCaseSensitivityInheritance() throws Exception {
        // Case sensitive check
        FileReader fileReader = TestUtils.getFileReader("xml/dataSetTest.xml");
        final IDataSet caseSensitive = new FlatXmlDataSetBuilder().setCaseSensitiveTableNames(true).build(fileReader);
        SequenceTableFilter filter = new SequenceTableFilter(getExpectedNames(),
                caseSensitive.isCaseSensitiveTableNames());

        final FilteredDataSet caseSesitiveFilter = new FilteredDataSet(filter, caseSensitive);
        assertEquals("case sensitive inheritance", true, caseSesitiveFilter.isCaseSensitiveTableNames());

        // Case insensitive check
        fileReader = TestUtils.getFileReader("xml/dataSetTest.xml");
        final IDataSet caseInsensitive = new FlatXmlDataSetBuilder().setCaseSensitiveTableNames(false)
                .build(fileReader);
        SequenceTableFilter filter1 = new SequenceTableFilter(getExpectedNames(),
                caseInsensitive.isCaseSensitiveTableNames());

        final FilteredDataSet caseInsesitiveFilter = new FilteredDataSet(filter1, caseInsensitive);
        assertEquals("case insensitive inheritance", false, caseInsesitiveFilter.isCaseSensitiveTableNames());
    }
}
