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

package org.dbunit.assertion;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.FileReader;
import java.io.StringReader;
import java.math.BigDecimal;
import java.util.function.Predicate;

import org.dbunit.AbstractDatabaseTest;
import org.dbunit.DatabaseUnitException;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.CompositeDataSet;
import org.dbunit.dataset.CompositeTable;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.DataSetUtils;
import org.dbunit.dataset.DefaultDataSet;
import org.dbunit.dataset.DefaultTable;
import org.dbunit.dataset.FilteredDataSet;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.ITableMetaData;
import org.dbunit.dataset.datatype.DataType;
import org.dbunit.dataset.xml.FlatXmlDataSetBuilder;
import org.dbunit.dataset.xml.XmlDataSet;
import org.dbunit.operation.DatabaseOperation;
import org.dbunit.testutil.TestUtils;
import org.junit.Test;

/**
 * @author Manuel Laflamme
 * @version $Revision$
 * @since Mar 22, 2002
 */
public class DbUnitAssertIT extends AbstractDatabaseTest {

    private DbUnitAssert assertion = new DbUnitAssert();

    public DbUnitAssertIT() throws Exception {
    }

    private IDataSet getDataSet() throws Exception {
        return new FlatXmlDataSetBuilder().build(new FileReader("src/test/resources/xml/assertionTest.xml"));
    }

    ////////////////////////////////////////////////////////////////////////////
    // Test methods

    @Test
    public void testAssertTablesEquals() throws Exception {
        IDataSet dataSet = getDataSet();
        assertion.assertEquals(dataSet.getTable("TEST_TABLE"), dataSet.getTable("TEST_TABLE_WITH_SAME_VALUE"),
                new Column[] { new Column("COLUMN0", DataType.VARCHAR) }, (Predicate<Column>) c -> false);
    }

    @Test
    public void testAssertTablesEmtpyEquals() throws Exception {
        IDataSet empty1 = new XmlDataSet(TestUtils.getFileReader("xml/assertionTest-empty1.xml"));
        IDataSet empty2 = new FlatXmlDataSetBuilder().build(TestUtils.getFileReader("xml/assertionTest-empty2.xml"));
        assertion.assertEquals(empty1, empty2);
    }

    @Test
    public void testAssertTablesEqualsColumnNamesCaseInsensitive() throws Exception {
        IDataSet dataSet = getDataSet();
        assertion.assertEquals(dataSet.getTable("TEST_TABLE"), dataSet.getTable("TEST_TABLE_WITH_LOWER_COLUMN_NAMES"),
                (Predicate<Column>) c -> false);
    }

    @Test
    public void testAssertTablesAndNamesNotEquals() throws Exception {
        IDataSet dataSet = getDataSet();
        assertion.assertEquals(dataSet.getTable("TEST_TABLE"), dataSet.getTable("TEST_TABLE_WITH_DIFFERENT_NAME"),
                (Predicate<Column>) c -> false);
    }

    @Test
    public void testAssertTablesAndColumnCountNotEquals() throws Exception {
        IDataSet dataSet = getDataSet();

        try {
            assertion.assertEquals(dataSet.getTable("TEST_TABLE"), dataSet.getTable("TEST_TABLE_WITH_3_COLUMNS"),
                    (Predicate<Column>) c -> false);
            throw new IllegalStateException("Should throw an AssertionFailedError");
        } catch (ComparisonFailure expected) {
            assertEquals("[COLUMN0, COLUMN1, COLUMN2, COLUMN3]", expected.getExpected());
            assertEquals("[COLUMN0, COLUMN1, COLUMN2]", expected.getActual());
            String expectedMsg = "column count (table=TEST_TABLE, expectedColCount=4, actualColCount=3) expected:<...N0, COLUMN1, COLUMN2[, COLUMN3]]> but was:<...N0, COLUMN1, COLUMN2[]]>";
            assertEquals(expectedMsg, expected.getMessage());
        }
    }

    @Test
    public void testAssertTablesAndColumnSequenceNotEquals() throws Exception {
        IDataSet dataSet = getDataSet();

        assertion.assertEquals(dataSet.getTable("TEST_TABLE"),
                dataSet.getTable("TEST_TABLE_WITH_DIFFERENT_COLUMN_SEQUENCE"), (Predicate<Column>) c -> false);
    }

    @Test
    public void testAssertTablesAndColumnNamesNotEquals() throws Exception {
        IDataSet dataSet = getDataSet();

        try {
            assertion.assertEquals(dataSet.getTable("TEST_TABLE"),
                    dataSet.getTable("TEST_TABLE_WITH_DIFFERENT_COLUMN_NAMES"), (Predicate<Column>) c -> false);
            throw new IllegalStateException("Should throw an AssertionFailedError");
        } catch (ComparisonFailure expected) {
            assertEquals("[COLUMN0, COLUMN1, COLUMN2, COLUMN3]", expected.getExpected());
            assertEquals("[COLUMN4, COLUMN5, COLUMN6, COLUMN7]", expected.getActual());
            String expectedMsg = "column mismatch (table=TEST_TABLE) expected:<[COLUMN[0, COLUMN1, COLUMN2, COLUMN3]]> but was:<[COLUMN[4, COLUMN5, COLUMN6, COLUMN7]]>";
            assertEquals(expectedMsg, expected.getMessage());
        }
    }

    @Test
    public void testAssertTablesAndRowCountNotEquals() throws Exception {
        IDataSet dataSet = getDataSet();

        try {
            assertion.assertEquals(dataSet.getTable("TEST_TABLE"), dataSet.getTable("TEST_TABLE_WITH_ONE_ROW"),
                    (Predicate<Column>) c -> false);
            throw new IllegalStateException("Should throw an AssertionFailedError");
        } catch (ComparisonFailure expected) {
            assertEquals("2", expected.getExpected());
            assertEquals("1", expected.getActual());
            String expectedMsg = "row count (table=TEST_TABLE) expected:<[2]> but was:<[1]>";
            assertEquals(expectedMsg, expected.getMessage());
        }
    }

    @Test
    public void testAssertTablesAndValuesNotEquals() throws Exception {
        IDataSet dataSet = getDataSet();

        try {
            assertion.assertEquals(dataSet.getTable("TEST_TABLE"), dataSet.getTable("TEST_TABLE_WITH_WRONG_VALUE"),
                    (Predicate<Column>) c -> false);
            throw new IllegalStateException("Should throw an AssertionFailedError");
        } catch (ComparisonFailure expected) {
            assertEquals("row 1 col 2", expected.getExpected());
            assertEquals("wrong value", expected.getActual());
            String expectedMsg = "value (table=TEST_TABLE, row=1, col=COLUMN2) expected:<[row 1 col 2]> but was:<[wrong value]>";
            assertEquals(expectedMsg, expected.getMessage());
        }
    }

    @Test
    public void assertEqualsIgnoreCols_tables_matching() throws DatabaseUnitException {
        TestTable expectedTable = new TestTable("TABLE1", "A", "B", "C");
        expectedTable.addRow("1", "2", "3");
        expectedTable.addRow("4", "5", "6");
        TestTable actualTable = new TestTable("TABLE1", "A", "B", "C");
        actualTable.addRow("1", "2", "3");
        actualTable.addRow("4", "5", "6");

        assertion.assertEqualsIgnoreCols(expectedTable, actualTable, new String[] {});
    }

    @Test
    public void assertEqualsIgnoreCols_tables_differentTableName() throws DatabaseUnitException {
        TestTable expectedTable = new TestTable("TABLE1", "A", "B", "C");
        TestTable actualTable = new TestTable("TABLE2", "A", "B", "C");

        // TODO tables names currently not compared
        assertion.assertEqualsIgnoreCols(expectedTable, actualTable, new String[] {});
    }

    @Test
    public void assertEqualsIgnoreCols_tables_extraColumn() throws DatabaseUnitException {
        TestTable expectedTable = new TestTable("TABLE1", "A", "B");
        expectedTable.addRow("1", "2");
        TestTable actualTable = new TestTable("TABLE1", "A", "B", "C");
        actualTable.addRow("1", "2", "3");

        try {
            assertion.assertEqualsIgnoreCols(expectedTable, actualTable, new String[] {});
            fail();
        } catch (ComparisonFailure exc) {
            assertEquals("[A, B]", exc.getExpected());
            assertEquals("[A, B, C]", exc.getActual());
            assertEquals(
                    "column count (table=TABLE1, expectedColCount=2, actualColCount=3) expected:<[A, B[]]> but was:<[A, B[, C]]>",
                    exc.getMessage());
        }
    }

    @Test
    public void assertEqualsIgnoreCols_tables_extraColumn_filtered() throws DatabaseUnitException {
        TestTable expectedTable = new TestTable("TABLE1", "A", "B");
        expectedTable.addRow("1", "2");
        TestTable actualTable = new TestTable("TABLE1", "A", "B", "C");
        actualTable.addRow("1", "2", "3");

        assertion.assertEqualsIgnoreCols(expectedTable, actualTable, new String[] { "C" });
    }

    @Test
    public void assertEqualsIgnoreCols_tables_missingColumn() throws DatabaseUnitException {
        TestTable expectedTable = new TestTable("TABLE1", "A", "B", "C");
        expectedTable.addRow("1", "2", "3");
        TestTable actualTable = new TestTable("TABLE1", "A", "B");
        actualTable.addRow("1", "2");

        try {
            assertion.assertEqualsIgnoreCols(expectedTable, actualTable, new String[] {});
            fail();
        } catch (ComparisonFailure exc) {
            assertEquals("[A, B, C]", exc.getExpected());
            assertEquals("[A, B]", exc.getActual());
            assertEquals(
                    "column count (table=TABLE1, expectedColCount=3, actualColCount=2) expected:<[A, B[, C]]> but was:<[A, B[]]>",
                    exc.getMessage());
        }
    }

    @Test
    public void assertEqualsIgnoreCols_tables_missingColumn_filtered() throws DatabaseUnitException {
        TestTable expectedTable = new TestTable("TABLE1", "A", "B", "C");
        expectedTable.addRow("1", "2", "3");
        TestTable actualTable = new TestTable("TABLE1", "A", "B");
        actualTable.addRow("1", "2");

        assertion.assertEqualsIgnoreCols(expectedTable, actualTable, new String[] { "C" });
    }

    @Test
    public void assertEqualsIgnoreCols_tables_mismatchedColumn() throws DatabaseUnitException {
        TestTable expectedTable = new TestTable("TABLE1", "A", "B", "C");
        expectedTable.addRow("1", "2", "3");
        TestTable actualTable = new TestTable("TABLE1", "A", "B", "D");
        actualTable.addRow("1", "2");

        try {
            assertion.assertEqualsIgnoreCols(expectedTable, actualTable, new String[] {});
            fail();
        } catch (ComparisonFailure exc) {
            assertEquals("[A, B, C]", exc.getExpected());
            assertEquals("[A, B, D]", exc.getActual());
            assertEquals("column mismatch (table=TABLE1) expected:<[A, B, [C]]> but was:<[A, B, [D]]>",
                    exc.getMessage());
        }
    }

    @Test
    public void assertEqualsIgnoreCols_tables_mismatchedColumn_filtered() throws DatabaseUnitException {
        TestTable expectedTable = new TestTable("TABLE1", "A", "B", "C");
        expectedTable.addRow("1", "2", "3");
        TestTable actualTable = new TestTable("TABLE1", "A", "B", "D");
        actualTable.addRow("1", "2");

        assertion.assertEqualsIgnoreCols(expectedTable, actualTable, new String[] { "C", "D" });
    }

    @Test
    public void assertEqualsIgnoreCols_tables_extraRow() throws DatabaseUnitException {
        TestTable expectedTable = new TestTable("TABLE1", "A", "B", "C");
        expectedTable.addRow("1", "2", "3");
        TestTable actualTable = new TestTable("TABLE1", "A", "B", "C");
        actualTable.addRow("1", "2", "3");
        actualTable.addRow("4", "5", "6");

        try {
            assertion.assertEqualsIgnoreCols(expectedTable, actualTable, new String[] {});
            fail();
        } catch (ComparisonFailure exc) {
            assertEquals("1", exc.getExpected());
            assertEquals("2", exc.getActual());
            assertEquals("row count (table=TABLE1) expected:<[1]> but was:<[2]>", exc.getMessage());
        }
    }

    @Test
    public void assertEqualsIgnoreCols_tables_missingRow() throws DatabaseUnitException {
        TestTable expectedTable = new TestTable("TABLE1", "A", "B", "C");
        expectedTable.addRow("1", "2", "3");
        expectedTable.addRow("4", "5", "6");
        TestTable actualTable = new TestTable("TABLE1", "A", "B", "C");
        actualTable.addRow("1", "2", "3");

        try {
            assertion.assertEqualsIgnoreCols(expectedTable, actualTable, new String[] {});
            fail();
        } catch (ComparisonFailure exc) {
            assertEquals("2", exc.getExpected());
            assertEquals("1", exc.getActual());
            assertEquals("row count (table=TABLE1) expected:<[2]> but was:<[1]>", exc.getMessage());
        }
    }

    @Test
    public void assertEqualsIgnoreCols_tables_mismatchedRow() throws DatabaseUnitException {
        TestTable expectedTable = new TestTable("TABLE1", "A", "B", "C");
        expectedTable.addRow("1", "2", "3");
        expectedTable.addRow("4", "5", "6");
        TestTable actualTable = new TestTable("TABLE1", "A", "B", "C");
        actualTable.addRow("1", "2", "3");
        actualTable.addRow("4", "5", "9");

        try {
            assertion.assertEqualsIgnoreCols(expectedTable, actualTable, new String[] {});
            fail();
        } catch (ComparisonFailure exc) {
            assertEquals("6", exc.getExpected());
            assertEquals("9", exc.getActual());
            assertEquals("value (table=TABLE1, row=1, col=C) expected:<[6]> but was:<[9]>", exc.getMessage());
        }
    }

    @Test
    public void assertEqualsIgnoreCols_tables_mismatchedRow_filtered() throws DatabaseUnitException {
        TestTable expectedTable = new TestTable("TABLE1", "A", "B", "C");
        expectedTable.addRow("1", "2", "3");
        expectedTable.addRow("4", "5", "6");
        TestTable actualTable = new TestTable("TABLE1", "A", "B", "C");
        actualTable.addRow("1", "2", "3");
        actualTable.addRow("4", "5", "9");

        assertion.assertEqualsIgnoreCols(expectedTable, actualTable, new String[] { "C" });
    }

    @Test
    public void testAssertTablesWithColFilterAndValuesNotEqualExcluded() throws Exception {
        IDataSet dataSet = getDataSet();
        ITable expectedTable = dataSet.getTable("TEST_TABLE");
        ITable actualTable = dataSet.getTable("TEST_TABLE_WITH_WRONG_VALUE");

        // Column2 has the wrong value, so exclude -> test should run successfully
        String[] allColumnsThatAreNotEqual = { "COLUMN2" };
        assertion.assertEqualsIgnoreCols(expectedTable, actualTable, allColumnsThatAreNotEqual);
    }

    @Test
    public void testAssertTablesWithColFilterAndValuesNotEqualNotExcluded() throws Exception {
        IDataSet dataSet = getDataSet();

        // Column0 has correct value. Column2 has the wrong value but is not filtered.
        // -> test should fail
        String[] filteredColumns = { "COLUMN0" };
        try {
            assertion.assertEqualsIgnoreCols(dataSet.getTable("TEST_TABLE"),
                    dataSet.getTable("TEST_TABLE_WITH_WRONG_VALUE"), filteredColumns);
            throw new IllegalStateException("Should throw an AssertionFailedError");
        } catch (ComparisonFailure expected) {
            assertEquals("row 1 col 2", expected.getExpected());
            assertEquals("wrong value", expected.getActual());
            String expectedMsg = "value (table=TEST_TABLE, row=1, col=COLUMN2) expected:<[row 1 col 2]> but was:<[wrong value]>";
            assertEquals(expectedMsg, expected.getMessage());
        }
    }

    @Test
    public void testAssertTablesAndValuesNotEquals_AdditionalColumnInfo() throws Exception {
        IDataSet dataSet = getDataSet();

        try {
            Column[] additionalColInfo = { new Column("COLUMN0", DataType.VARCHAR) };
            final Column[] additionalColumnInfo = additionalColInfo;
            assertion.assertEquals(dataSet.getTable("TEST_TABLE"), dataSet.getTable("TEST_TABLE_WITH_WRONG_VALUE"),
                    additionalColumnInfo, (Predicate<Column>) c -> false);
            throw new IllegalStateException("Should throw an AssertionFailedError");
        } catch (ComparisonFailure expected) {
            String expectedMsg = "org.dbunit.assertion.ComparisonFailure: value (table=TEST_TABLE, row=1, col=COLUMN2, "
                    + "Additional row info: ('COLUMN0': expected=<row 1 col 0>, actual=<row 1 col 0>)) "
                    + "expected:<[row 1 col 2]> but was:<[wrong value]>";
            String actualMsg = expected.toString();
            assertEquals("row 1 col 2", expected.getExpected());
            assertEquals("wrong value", expected.getActual());
            assertEquals("Exception message did not match the expected one.", expectedMsg, actualMsg);
        }
    }

    @Test
    public void testAssertTablesEqualsAndIncompatibleDataType() throws Exception {
        String tableName = "TABLE_NAME";

        // Setup actual table
        Column[] actualColumns = { new Column("BOOLEAN", DataType.BOOLEAN), };
        Object[] actualRow = { Boolean.TRUE, };
        DefaultTable actualTable = new DefaultTable(tableName, actualColumns);
        actualTable.addRow(actualRow);

        // Setup expected table
        Column[] expectedColumns = { new Column("BOOLEAN", DataType.VARCHAR), };
        Object[] expectedRow = { "1", };
        DefaultTable expectedTable = new DefaultTable(tableName, expectedColumns);
        expectedTable.addRow(expectedRow);

        try {
            final ITable expectedTable1 = expectedTable;
            final ITable actualTable1 = actualTable;
            assertion.assertEquals(expectedTable1, actualTable1, (Predicate<Column>) c -> false);
        } catch (ComparisonFailure expected) {
            assertEquals("VARCHAR", expected.getExpected());
            assertEquals("BOOLEAN", expected.getActual());
            String expectedMsg = "Incompatible data types: (table=TABLE_NAME, col=BOOLEAN) expected:<[VARCHAR]> but was:<[BOOLEAN]>";
            assertEquals(expectedMsg, expected.getMessage());
        }
    }

    @Test
    public void testAssertTablesByQueryWithColFilterAndValuesNotEqualExcluded() throws Exception {
        IDatabaseConnection connection = database.getConnection();

        IDataSet dataSet = environment.getInitDataSet();
        ITable expectedTable = dataSet.getTable("TEST_TABLE");

        ITable table = dataSet.getTable("TEST_TABLE");
        ITable filteredTable = new ModifyingTable(table, "COLUMN2");
        DatabaseOperation.CLEAN_INSERT.execute(connection, new DefaultDataSet(filteredTable));

        // Ignore COLUMN2 which has been modified by the "ModifyingTable" above and
        // hence does not match.
        // When we ignore this column, the assertion should work without failure
        String[] ignoreCols = { "COLUMN2" };
        assertion.assertEqualsByQuery(expectedTable, connection, "TEST_TABLE", "select * from TEST_TABLE order by 1",
                ignoreCols);
    }

    @Test
    public void testAssertTablesByQueryWithColFilterAndValuesNotEqualNotExcluded() throws Exception {
        IDatabaseConnection connection = database.getConnection();

        IDataSet dataSet = environment.getInitDataSet();
        ITable expectedTable = dataSet.getTable("TEST_TABLE");

        ITable table = dataSet.getTable("TEST_TABLE");
        ITable filteredTable = new ModifyingTable(table, "COLUMN2");
        DatabaseOperation.CLEAN_INSERT.execute(connection, new DefaultDataSet(filteredTable));

        // Ignore COLUMN1 which has NOT been modified by the "ModifyingTable". The
        // modified COLUMN2 does
        // not match and is not ignored. So the assertion should fail.
        String[] ignoreCols = { "COLUMN1" };
        try {
            assertion.assertEqualsByQuery(expectedTable, connection, "TEST_TABLE",
                    "select * from TEST_TABLE order by 1", ignoreCols);
            fail("The assertion should not work");
        } catch (ComparisonFailure expected) {
            assertEquals("row 0 col 2", expected.getExpected());
            assertEquals("row 0 col 2 (modified COLUMN2)", expected.getActual());
            String expectedMsg = "value (table=TEST_TABLE, row=0, col=COLUMN2) expected:<row 0 col 2[]> but was:<row 0 col 2[ (modified COLUMN2)]>";
            assertEquals(expectedMsg, expected.getMessage());
        }
    }

    @Test
    public void testAssertTablesEqualsAndCompatibleDataType() throws Exception {
        String tableName = "TABLE_NAME";
        java.sql.Timestamp now = new java.sql.Timestamp(System.currentTimeMillis());

        // Setup actual table
        Column[] actualColumns = { new Column("BOOLEAN", DataType.BOOLEAN), new Column("TIMESTAMP", DataType.TIMESTAMP),
                new Column("STRING", DataType.CHAR), new Column("NUMERIC", DataType.NUMERIC), };
        Object[] actualRow = { Boolean.TRUE, now, "0", new BigDecimal("123.4"), };
        DefaultTable actualTable = new DefaultTable(tableName, actualColumns);
        actualTable.addRow(actualRow);

        // Setup expected table
        Column[] expectedColumns = { new Column("BOOLEAN", DataType.UNKNOWN), new Column("TIMESTAMP", DataType.UNKNOWN),
                new Column("STRING", DataType.UNKNOWN), new Column("NUMERIC", DataType.UNKNOWN), };
        Object[] expectedRow = { "1", new Long(now.getTime()), new Integer("0"), "123.4000", };
        DefaultTable expectedTable = new DefaultTable(tableName, expectedColumns);
        expectedTable.addRow(expectedRow);
        final ITable expectedTable1 = expectedTable;
        final ITable actualTable1 = actualTable;

        assertion.assertEquals(expectedTable1, actualTable1, (Predicate<Column>) c -> false);
    }

    @Test
    public void testAssertDataSetsEquals() throws Exception {
        IDataSet dataSet1 = getDataSet();

        // change table names order
        String[] names = DataSetUtils.getReverseTableNames(dataSet1);
        IDataSet dataSet2 = new FilteredDataSet(names, dataSet1);

        assertTrue("Datasets are the same instances.", dataSet1 != dataSet2);
        assertion.assertEquals(dataSet1, dataSet2);
    }

    @Test
    public void testAssertDataSetsEqualsTableNamesCaseInsensitive() throws Exception {
        IDataSet dataSet1 = getDataSet();

        // change table names case
        String[] names = dataSet1.getTableNames();
        for (int i = 0; i < names.length; i++) {
            names[i] = names[i].toLowerCase();
        }
        IDataSet dataSet2 = new FilteredDataSet(names, dataSet1);

        assertTrue("Datasets are the same instances.", dataSet1 != dataSet2);
        assertion.assertEquals(dataSet1, dataSet2);
    }

    @Test
    public void testAssertDataSetsTableNamesCaseSensitiveNotEquals() throws Exception {
        IDataSet dataSet1 = new FlatXmlDataSetBuilder().setCaseSensitiveTableNames(true)
                .build(TestUtils.getFileReader("xml/assertion_table_name_case_sensitive_1.xml"));
        IDataSet dataSet2 = new FlatXmlDataSetBuilder().setCaseSensitiveTableNames(true)
                .build(TestUtils.getFileReader("xml/assertion_table_name_case_sensitive_2.xml"));

        try {
            assertion.assertEquals(dataSet1, dataSet2);
            fail("Should throw an AssertionFailedError");
        } catch (ComparisonFailure expected) {
            assertEquals("Expected table name did not match.", "[TEST_TABLE_WITH_CASE_SENSITIVE_NAME]",
                    expected.getExpected());
            assertEquals("Actual table name did not match.", "[test_table_with_case_sensitive_name]",
                    expected.getActual());
        }
    }

    @Test
    public void testAssertDataSetsTableNamesCaseSensitiveWithLowerCaseEquals() throws Exception {
        IDataSet dataSet1 = new FlatXmlDataSetBuilder().setCaseSensitiveTableNames(true)
                .build(TestUtils.getFileReader("xml/assertion_table_name_case_sensitive_with_lower_case.xml"));
        IDataSet dataSet2 = new FlatXmlDataSetBuilder().setCaseSensitiveTableNames(true)
                .build(TestUtils.getFileReader("xml/assertion_table_name_case_sensitive_with_lower_case.xml"));
        assertTrue("Datasets are the same instances.", dataSet1 != dataSet2);
        assertion.assertEquals(dataSet1, dataSet2);
    }

    @Test
    public void testAssertDataSetsAndTableCountNotEquals() throws Exception {
        IDataSet dataSet1 = getDataSet();

        // only one table
        String[] names = { dataSet1.getTableNames()[0] };
        IDataSet dataSet2 = new FilteredDataSet(names, dataSet1);

        assertTrue("Datasets are the same instances.", dataSet1 != dataSet2);

        try {
            assertion.assertEquals(dataSet1, dataSet2);
            throw new IllegalStateException("Should throw an AssertionFailedError");
        } catch (ComparisonFailure expected) {
            assertEquals("10", expected.getExpected());
            assertEquals("1", expected.getActual());
            assertEquals("table count expected:<1[0]> but was:<1[]>", expected.getMessage());
        }
    }

    @Test
    public void testAssertDataSetsAndTableNamesNotEquals() throws Exception {
        IDataSet dataSet1 = getDataSet();

        // reverse table names
        String[] names = dataSet1.getTableNames();
        ITable[] tables = new ITable[names.length];
        for (int i = 0; i < names.length; i++) {
            String reversedName = new StringBuffer(names[i]).reverse().toString();
            tables[i] = new CompositeTable(reversedName, dataSet1.getTable(names[i]));
        }
        IDataSet dataSet2 = new DefaultDataSet(tables);

        assertTrue("Datasets are the same instances.", dataSet1 != dataSet2);
        assertEquals("table count", dataSet1.getTableNames().length, dataSet2.getTableNames().length);

        try {
            assertion.assertEquals(dataSet1, dataSet2);
            throw new IllegalStateException("Should throw an AssertionFailedError");
        } catch (ComparisonFailure expected) {
        }
    }

    @Test
    public void testAssertDataSetsAndTablesNotEquals() throws Exception {
        IDataSet dataSet1 = getDataSet();

        // different row counts (double)
        IDataSet dataSet2 = new CompositeDataSet(dataSet1, dataSet1);

        assertTrue("Datasets are the same instances.", dataSet1 != dataSet2);
        assertEquals("table count", dataSet1.getTableNames().length, dataSet2.getTableNames().length);

        try {
            assertion.assertEquals(dataSet1, dataSet2);
            throw new IllegalStateException("Should throw an AssertionFailedError");
        } catch (ComparisonFailure expected) {
            assertEquals("2", expected.getExpected());
            assertEquals("4", expected.getActual());
            assertEquals("row count (table=TEST_TABLE) expected:<[2]> but was:<[4]>", expected.getMessage());
        }
    }

    @Test
    public void testAssertDataSetsWithFailureHandler() throws Exception {
        DiffCollectingFailureHandler fh = new DiffCollectingFailureHandler();

        String xml1 = "<dataset>\n" + "<TEST_TABLE COLUMN0='row 0 col 0' COLUMN1='row 0 col 1'/>\n" + "</dataset>\n";
        IDataSet dataSet1 = new FlatXmlDataSetBuilder().build(new StringReader(xml1));
        String xml2 = "<dataset>\n"
                + "<TEST_TABLE COLUMN0='row 0 col somthing' COLUMN1='row 0 col something mysterious'/>\n"
                + "</dataset>\n";
        IDataSet dataSet2 = new FlatXmlDataSetBuilder().build(new StringReader(xml2));

        // Invoke the assertion
        assertion.assertEquals(dataSet1, dataSet2, fh);
        // We expect that no failure was thrown even if the dataSets were not equal.
        // This is because our custom failureHandler
        assertEquals(2, fh.getDiffList().size());
    }

    @Test
    public void testGetComparisonDataType_ExpectedTypeUnknown() {
        Column expectedColumn = new Column("COL1", DataType.UNKNOWN);
        Column actualColumn = new Column("COL1", DataType.VARCHAR);
        DataType dataType = new DbUnitAssert.ComparisonColumn("BLABLA_TABLE_NOT_NEEDED_HERE", expectedColumn,
                actualColumn, assertion.getDefaultFailureHandler()).getDataType();
        assertEquals(DataType.VARCHAR, dataType);
    }

    @Test
    public void testGetComparisonDataType_ActualTypeUnknown() {
        Column expectedColumn = new Column("COL1", DataType.VARCHAR);
        Column actualColumn = new Column("COL1", DataType.UNKNOWN);
        DataType dataType = new DbUnitAssert.ComparisonColumn("BLABLA_TABLE_NOT_NEEDED_HERE", expectedColumn,
                actualColumn, assertion.getDefaultFailureHandler()).getDataType();
        assertEquals(DataType.VARCHAR, dataType);
    }

    @Test
    public void testGetComparisonDataType_BothTypesSetIncompatible() {
        Column expectedColumn = new Column("COL1", DataType.VARCHAR);
        Column actualColumn = new Column("COL1", DataType.NUMERIC);
        try {
            new DbUnitAssert.ComparisonColumn("BLABLA_TABLE_NOT_NEEDED_HERE", expectedColumn, actualColumn,
                    assertion.getDefaultFailureHandler()).getDataType();
            fail("Incompatible datatypes should not work");
        } catch (ComparisonFailure expected) {
            assertEquals("VARCHAR", expected.getExpected());
            assertEquals("NUMERIC", expected.getActual());
            String expectedMsg = "Incompatible data types: (table=BLABLA_TABLE_NOT_NEEDED_HERE, col=COL1) expected:<[VARCHAR]> but was:<[NUMERIC]>";
            assertEquals(expectedMsg, expected.getMessage());
        }
    }

    @Test
    public void testGetComparisonDataType_BothTypesSetToSame() {
        Column expectedColumn = new Column("COL1", DataType.VARCHAR);
        Column actualColumn = new Column("COL1", DataType.VARCHAR);
        DataType dataType = new DbUnitAssert.ComparisonColumn("BLABLA_TABLE_NOT_NEEDED_HERE", expectedColumn,
                actualColumn, assertion.getDefaultFailureHandler()).getDataType();
        assertEquals(DataType.VARCHAR, dataType);
    }

    @Test
    public void testGetComparisonDataType_BothTypesUnknown() {
        Column expectedColumn = new Column("COL1", DataType.UNKNOWN);
        Column actualColumn = new Column("COL1", DataType.UNKNOWN);
        DataType dataType = new DbUnitAssert.ComparisonColumn("BLABLA_TABLE_NOT_NEEDED_HERE", expectedColumn,
                actualColumn, assertion.getDefaultFailureHandler()).getDataType();
        assertEquals(DataType.UNKNOWN, dataType);
    }

    /**
     * Test utility that modifies all values for a specific column arbitrarily
     */
    protected static class ModifyingTable implements ITable {

        private ITable _wrappedTable;
        private String _columnToModify;

        public ModifyingTable(ITable originalTable, String columnToModify) {
            this._wrappedTable = originalTable;
            this._columnToModify = columnToModify;
        }

        @Override
        public int getRowCount() {
            return this._wrappedTable.getRowCount();
        }

        @Override
        public ITableMetaData getTableMetaData() {
            return this._wrappedTable.getTableMetaData();
        }

        @Override
        public Object getValue(int row, String column) throws DataSetException {
            Object originalValue = _wrappedTable.getValue(row, column);

            // Modify the value if column name matches
            if (column.equalsIgnoreCase(this._columnToModify)) {
                return String.valueOf(originalValue) + " (modified " + _columnToModify + ")";
            }
            return originalValue;
        }

    }

}
