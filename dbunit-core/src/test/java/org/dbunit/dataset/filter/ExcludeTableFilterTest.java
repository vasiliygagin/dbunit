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

import java.util.Arrays;

import org.dbunit.dataset.DataSetUtils;
import org.dbunit.dataset.DefaultDataSet;
import org.dbunit.dataset.DefaultTable;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.LowerCaseDataSet;
import org.junit.Test;

/**
 * @author Manuel Laflamme
 * @since Mar 18, 2003
 * @version $Revision$
 */
public class ExcludeTableFilterTest extends AbstractTableFilterTest {

    static final String MATCHING_NAME = "aBcDe";
    static final String[] MATCHING_PATTERNS = IncludeTableFilterTest.MATCHING_PATTERNS;
    static final String[] NONMATCHING_PATTERNS = IncludeTableFilterTest.NONMATCHING_PATTERNS;

    public ExcludeTableFilterTest() throws Exception {
    }

    @Override
    public void testAccept() throws Exception {
        String[] validNames = getExpectedNames();
        ExcludeTableFilter filter = new ExcludeTableFilter();
        filter.excludeTable(getExtraTableName());

        for (String validName : validNames) {
            assertEquals(validName, true, filter.accept(validName));
        }
    }

    @Override
    public void testIsCaseInsensitiveValidName() throws Exception {
        String[] validNames = getExpectedNames();
        ExcludeTableFilter filter = new ExcludeTableFilter();
        filter.excludeTable(getExtraTableName());

        for (String validName : validNames) {
            assertEquals(validName, true, filter.accept(validName));
        }
    }

    @Override
    public void testIsValidNameAndInvalid() throws Exception {
        String[] invalidNames = { "INVALID_TABLE", "UNKNOWN_TABLE", };
        ITableFilter filter = new ExcludeTableFilter(invalidNames);

        for (String invalidName : invalidNames) {
            assertEquals(invalidName, false, filter.accept(invalidName));
        }
    }

    @Override
    public void testGetTableNames() throws Exception {
        String[] expectedNames = getExpectedNames();
        ExcludeTableFilter filter = new ExcludeTableFilter();
        filter.excludeTable(getExtraTableName());

        IDataSet dataSet = createDataSet();
        assertTrue("dataset names count", dataSet.getTableNames().length > expectedNames.length);

        String[] actualNames = filter.getTableNames(dataSet);
        assertEquals("name count", expectedNames.length, actualNames.length);
        assertEquals("names", Arrays.asList(expectedNames), Arrays.asList(actualNames));
    }

    @Override
    public void testGetTableNamesAndTableNotInDecoratedDataSet() throws Exception {
        String[] expectedNames = getExpectedNames();
        ExcludeTableFilter filter = new ExcludeTableFilter();
        filter.excludeTable(getExtraTableName());
        filter.excludeTable("UNKNOWN_TABLE");

        IDataSet dataSet = createDataSet();
        assertTrue("dataset names count", dataSet.getTableNames().length > expectedNames.length);

        String[] actualNames = filter.getTableNames(dataSet);
        assertEquals("name count", expectedNames.length, actualNames.length);
        assertEquals("names", Arrays.asList(expectedNames), Arrays.asList(actualNames));
    }

    @Override
    public void testGetCaseInsensitiveTableNames() throws Exception {
        ExcludeTableFilter filter = new ExcludeTableFilter();
        filter.excludeTable(getExtraTableName());

        String[] expectedNames = getExpectedLowerNames();
        IDataSet dataSet = new LowerCaseDataSet(createDataSet());
        assertTrue("dataset names count", dataSet.getTableNames().length > expectedNames.length);

        String[] actualNames = filter.getTableNames(dataSet);
        assertEquals("name count", expectedNames.length, actualNames.length);
        assertEquals("names", Arrays.asList(expectedNames), Arrays.asList(actualNames));
    }

    @Override
    public void testGetReverseTableNames() throws Exception {
        // Cannot test!
    }

    @Override
    public void testIterator() throws Exception {
        String[] expectedNames = getExpectedNames();
        ExcludeTableFilter filter = new ExcludeTableFilter();
        filter.excludeTable(getExtraTableName());

        IDataSet dataSet = createDataSet();
        assertTrue("dataset names count", dataSet.getTableNames().length > expectedNames.length);

        ITable[] actualTables = DataSetUtils.getTables(filter.iterator(dataSet, false));
        String[] actualNames = new DefaultDataSet(actualTables).getTableNames();
        assertEquals("table count", expectedNames.length, actualTables.length);
        assertEquals("table names", Arrays.asList(expectedNames), Arrays.asList(actualNames));
    }

    @Override
    public void testCaseInsensitiveIterator() throws Exception {
        ExcludeTableFilter filter = new ExcludeTableFilter();
        filter.excludeTable(getExtraTableName());

        String[] expectedNames = getExpectedLowerNames();
        IDataSet dataSet = new LowerCaseDataSet(createDataSet());
        assertTrue("dataset names count", dataSet.getTableNames().length > expectedNames.length);

        ITable[] actualTables = DataSetUtils.getTables(filter.iterator(dataSet, false));
        String[] actualNames = new DefaultDataSet(actualTables).getTableNames();
        assertEquals("table count", expectedNames.length, actualTables.length);
        assertEquals("table names", Arrays.asList(expectedNames), Arrays.asList(actualNames));
    }

    @Override
    public void testReverseIterator() throws Exception {
        // Cannot test!
    }

    @Override
    public void testIteratorAndTableNotInDecoratedDataSet() throws Exception {
        String[] expectedNames = getExpectedNames();
        ExcludeTableFilter filter = new ExcludeTableFilter();
        filter.excludeTable(getExtraTableName());
        filter.excludeTable("UNKNOWN_TABLE");

        IDataSet dataSet = createDataSet();
        assertTrue("dataset names count", dataSet.getTableNames().length > expectedNames.length);

        ITable[] actualTables = DataSetUtils.getTables(filter.iterator(dataSet, false));
        String[] actualNames = new DefaultDataSet(actualTables).getTableNames();
        assertEquals("table count", expectedNames.length, actualTables.length);
        assertEquals("table names", Arrays.asList(expectedNames), Arrays.asList(actualNames));
    }

    ////////////////////////////////////////////////////////////////////////////

    @Test
    public void testIsValidNameWithPatterns() throws Exception {
        String validName = MATCHING_NAME;

        String[] patterns = NONMATCHING_PATTERNS;
        for (String pattern : patterns) {
            ExcludeTableFilter filter = new ExcludeTableFilter();
            filter.excludeTable(pattern);
            assertEquals(pattern, true, filter.accept(validName));
        }
    }

    @Test
    public void testIsValidNameInvalidWithPatterns() throws Exception {
        String validName = MATCHING_NAME;

        String[] patterns = MATCHING_PATTERNS;
        for (String pattern : patterns) {
            ExcludeTableFilter filter = new ExcludeTableFilter();
            filter.excludeTable(pattern);
            assertEquals(pattern, false, filter.accept(validName));
        }
    }

    @Test
    public void testGetTableNamesWithPatterns() throws Exception {
        String nonMatchingName = "toto titi tata";
        String[] expectedNames = { nonMatchingName };
        IDataSet dataSet = new DefaultDataSet(
                new ITable[] { new DefaultTable(MATCHING_NAME), new DefaultTable(nonMatchingName), });
        assertTrue("dataset names count", dataSet.getTableNames().length > expectedNames.length);

        String[] patterns = MATCHING_PATTERNS;
        for (String pattern : patterns) {
            ExcludeTableFilter filter = new ExcludeTableFilter();
            filter.excludeTable(pattern);

            // this pattern match everything, so ensure everything filtered
            if (pattern.equals("*")) {
                String[] actualNames = filter.getTableNames(dataSet);
                assertEquals("name count - " + pattern, 0, actualNames.length);
            } else {
                String[] actualNames = filter.getTableNames(dataSet);
                assertEquals("name count - " + pattern, expectedNames.length, actualNames.length);
                assertEquals("names - " + pattern, Arrays.asList(expectedNames), Arrays.asList(actualNames));
            }
        }
    }

    @Test
    public void testGetTableNamesWithNonMatchingPatterns() throws Exception {
        String[] expectedNames = { MATCHING_NAME };
        IDataSet dataSet = new DefaultDataSet(new ITable[] { new DefaultTable(MATCHING_NAME), });

        String[] patterns = NONMATCHING_PATTERNS;
        for (String pattern : patterns) {
            ExcludeTableFilter filter = new ExcludeTableFilter();
            filter.excludeTable(pattern);

            String[] actualNames = filter.getTableNames(dataSet);
            assertEquals("name count - " + pattern, expectedNames.length, actualNames.length);
            assertEquals("names - " + pattern, Arrays.asList(expectedNames), Arrays.asList(actualNames));
        }
    }

    @Test
    public void testGetTablesWithPatterns() throws Exception {
        String nonMatchingName = "toto titi tata";
        String[] expectedNames = { nonMatchingName };
        IDataSet dataSet = new DefaultDataSet(
                new ITable[] { new DefaultTable(MATCHING_NAME), new DefaultTable(nonMatchingName), });
        assertTrue("dataset names count", dataSet.getTableNames().length > expectedNames.length);

        String[] patterns = MATCHING_PATTERNS;
        for (String pattern : patterns) {
            ExcludeTableFilter filter = new ExcludeTableFilter();
            filter.excludeTable(pattern);

            // this pattern match everything, so ensure everything is filtered
            if (pattern.equals("*")) {
                ITable[] actualTables = DataSetUtils.getTables(filter.iterator(dataSet, false));
                String[] actualNames = new DefaultDataSet(actualTables).getTableNames();
                assertEquals("table count - " + pattern, 0, actualNames.length);
            } else {
                ITable[] actualTables = DataSetUtils.getTables(filter.iterator(dataSet, false));
                String[] actualNames = new DefaultDataSet(actualTables).getTableNames();
                assertEquals("table count - " + pattern, expectedNames.length, actualTables.length);
                assertEquals("table names - " + pattern, Arrays.asList(expectedNames), Arrays.asList(actualNames));
            }
        }
    }

    @Test
    public void testGetTablesWithNonMatchingPatterns() throws Exception {
        String[] expectedNames = { MATCHING_NAME };
        IDataSet dataSet = new DefaultDataSet(new ITable[] { new DefaultTable(MATCHING_NAME), });
        assertTrue("dataset names count", dataSet.getTableNames().length > 0);

        String[] patterns = NONMATCHING_PATTERNS;
        for (String pattern : patterns) {
            ExcludeTableFilter filter = new ExcludeTableFilter();
            filter.excludeTable(pattern);

            ITable[] actualTables = DataSetUtils.getTables(filter.iterator(dataSet, false));
            String[] actualNames = new DefaultDataSet(actualTables).getTableNames();
            assertEquals("table count - " + pattern, expectedNames.length, actualTables.length);
            assertEquals("table names - " + pattern, Arrays.asList(expectedNames), Arrays.asList(actualNames));
        }
    }

}
