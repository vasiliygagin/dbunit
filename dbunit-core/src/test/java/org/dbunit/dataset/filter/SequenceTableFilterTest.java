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
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.dbunit.dataset.DataSetUtils;
import org.dbunit.dataset.DefaultDataSet;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.LowerCaseDataSet;
import org.junit.Test;

/**
 * @author Manuel Laflamme
 * @author Last changed by: $Author$
 * @version $Revision$ $Date$
 * @since 2.2.0
 */
public class SequenceTableFilterTest extends AbstractTableFilterTest {

    public SequenceTableFilterTest() throws Exception {
    }

    @Override
    @Test
    public void testAccept() throws Exception {
        String[] validNames = getExpectedNames();
        ITableFilter filter = new SequenceTableFilter(validNames);

        for (String validName : validNames) {
            assertEquals(validName, true, filter.accept(validName));
        }
    }

    @Override
    @Test
    public void testIsCaseInsensitiveValidName() throws Exception {
        String[] validNames = getExpectedNames();
        ITableFilter filter = new SequenceTableFilter(validNames);

        for (String validName : validNames) {
            assertEquals(validName, true, filter.accept(validName));
        }
    }

    @Override
    @Test
    public void testIsValidNameAndInvalid() throws Exception {
        String[] invalidNames = { "INVALID_TABLE", "UNKNOWN_TABLE", };
        String[] validNames = getExpectedNames();
        ITableFilter filter = new SequenceTableFilter(validNames);

        for (String invalidName : invalidNames) {
            assertEquals(invalidName, false, filter.accept(invalidName));
        }
    }

    @Override
    @Test
    public void testGetTableNames() throws Exception {
        String[] expectedNames = getExpectedNames();
        ITableFilter filter = new SequenceTableFilter(expectedNames);

        IDataSet dataSet = createDataSet();
        assertTrue("dataset names count", dataSet.getTableNames().length > expectedNames.length);

        String[] actualNames = filter.getTableNames(dataSet);
        assertEquals("name count", expectedNames.length, actualNames.length);
        assertEquals("names", Arrays.asList(expectedNames), Arrays.asList(actualNames));
    }

    @Override
    @Test
    public void testGetTableNamesAndTableNotInDecoratedDataSet() throws Exception {
        String[] expectedNames = getExpectedNames();

        List filterNameList = new ArrayList(Arrays.asList(expectedNames));
        filterNameList.add("UNKNOWN_TABLE");
        String[] filterNames = (String[]) filterNameList.toArray(new String[0]);
        ITableFilter filter = new SequenceTableFilter(filterNames);

        IDataSet dataSet = createDataSet();
        assertTrue("dataset names count", dataSet.getTableNames().length > expectedNames.length);

        String[] actualNames = filter.getTableNames(dataSet);
        assertEquals("name count", expectedNames.length, actualNames.length);
        assertEquals("names", Arrays.asList(expectedNames), Arrays.asList(actualNames));
    }

    @Override
    @Test
    public void testGetCaseInsensitiveTableNames() throws Exception {
        String[] filterNames = getExpectedNames();
        ITableFilter filter = new SequenceTableFilter(filterNames);

        String[] expectedNames = getExpectedLowerNames();
        IDataSet dataSet = new LowerCaseDataSet(createDataSet());
        assertTrue("dataset names count", dataSet.getTableNames().length > expectedNames.length);

        String[] actualNames = filter.getTableNames(dataSet);
        assertEquals("name count", expectedNames.length, actualNames.length);
        assertEquals("names", Arrays.asList(expectedNames), Arrays.asList(actualNames));
    }

    @Override
    @Test
    public void testGetReverseTableNames() throws Exception {
        String[] expectedNames = DataSetUtils.reverseStringArray(getExpectedNames());
        ITableFilter filter = new SequenceTableFilter(expectedNames);

        IDataSet dataSet = createDataSet();
        assertTrue("dataset names count", dataSet.getTableNames().length > expectedNames.length);

        String[] actualNames = filter.getTableNames(dataSet);
        assertEquals("name count", expectedNames.length, actualNames.length);
        assertEquals("names", Arrays.asList(expectedNames), Arrays.asList(actualNames));
    }

    @Override
    @Test
    public void testIterator() throws Exception {
        String[] expectedNames = getExpectedNames();
        ITableFilter filter = new SequenceTableFilter(expectedNames);

        IDataSet dataSet = createDataSet();
        assertTrue("dataset names count", dataSet.getTableNames().length > expectedNames.length);

        ITable[] actualTables = DataSetUtils.getTables(filter.iterator(dataSet, false));
        String[] actualNames = new DefaultDataSet(actualTables).getTableNames();
        assertEquals("table count", expectedNames.length, actualTables.length);
        assertEquals("table names", Arrays.asList(expectedNames), Arrays.asList(actualNames));
    }

    @Override
    @Test
    public void testCaseInsensitiveIterator() throws Exception {
        ITableFilter filter = new SequenceTableFilter(getExpectedNames());
        String[] lowerNames = getExpectedLowerNames();

        IDataSet dataSet = new LowerCaseDataSet(createDataSet());
        assertTrue("dataset names count", dataSet.getTableNames().length > lowerNames.length);

        ITable[] actualTables = DataSetUtils.getTables(filter.iterator(dataSet, false));
        String[] actualNames = new DefaultDataSet(actualTables).getTableNames();
        assertEquals("table count", lowerNames.length, actualTables.length);
        assertEquals("table names", Arrays.asList(lowerNames), Arrays.asList(actualNames));
    }

    @Override
    @Test
    public void testReverseIterator() throws Exception {
        String[] filterNames = getExpectedNames();
        String[] expectedNames = DataSetUtils.reverseStringArray(filterNames);
        ITableFilter filter = new SequenceTableFilter(filterNames);

        IDataSet dataSet = createDataSet();
        assertTrue("dataset names count", dataSet.getTableNames().length > expectedNames.length);

        ITable[] actualTables = DataSetUtils.getTables(filter.iterator(dataSet, true));
        String[] actualNames = new DefaultDataSet(actualTables).getTableNames();
        assertEquals("table count", expectedNames.length, actualTables.length);
        assertEquals("table names", Arrays.asList(expectedNames), Arrays.asList(actualNames));
    }

    @Override
    @Test
    public void testIteratorAndTableNotInDecoratedDataSet() throws Exception {
        String[] expectedNames = getExpectedNames();

        List filterNameList = new ArrayList(Arrays.asList(expectedNames));
        filterNameList.add("UNKNOWN_TABLE");
        String[] filterNames = (String[]) filterNameList.toArray(new String[0]);
        ITableFilter filter = new SequenceTableFilter(filterNames);

        IDataSet dataSet = createDataSet();
        assertTrue("dataset names count", dataSet.getTableNames().length > expectedNames.length);

        ITable[] actualTables = DataSetUtils.getTables(filter.iterator(dataSet, false));
        String[] actualNames = new DefaultDataSet(actualTables).getTableNames();
        assertEquals("table count", expectedNames.length, actualTables.length);
        assertEquals("table names", Arrays.asList(expectedNames), Arrays.asList(actualNames));
    }

    ////////////////////////////////////////////////////////////////////////////

    @Test
    public void testIteratorWithDifferentSequence() throws Exception {
        String[] expectedNames = DataSetUtils.reverseStringArray(getExpectedNames());
        ITableFilter filter = new SequenceTableFilter(expectedNames);

        IDataSet dataSet = createDataSet();
        assertTrue("dataset names count", dataSet.getTableNames().length > expectedNames.length);

        ITable[] actualTables = DataSetUtils.getTables(filter.iterator(dataSet, false));
        String[] actualNames = new DefaultDataSet(actualTables).getTableNames();
        assertEquals("table count", expectedNames.length, actualTables.length);
        assertEquals("table names", Arrays.asList(expectedNames), Arrays.asList(actualNames));
    }

}
