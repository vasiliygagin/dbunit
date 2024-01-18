package org.dbunit.assertion;

import static java.util.Arrays.asList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import org.dbunit.DatabaseUnitException;
import org.dbunit.assertion.DbUnitAssert.ComparisonColumn;
import org.dbunit.assertion.comparer.value.DefaultValueComparerDefaults;
import org.dbunit.assertion.comparer.value.ValueComparer;
import org.dbunit.assertion.comparer.value.ValueComparerDefaults;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.Columns;
import org.dbunit.dataset.ColumnsComparer;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.ITableMetaData;
import org.dbunit.dataset.datatype.DataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for DbUnit assert classes containing common methods.
 *
 * @author Jeff Jensen
 * @since 2.6.0
 */
public class DbUnitAssertBase {

    public final Logger log = LoggerFactory.getLogger(DbUnitAssertBase.class);

    private FailureHandler junitFailureFactory = getJUnitFailureFactory();

    public final DefaultValueComparerDefaults valueComparerDefaults = new DefaultValueComparerDefaults();
    protected final ColumnsComparer columnComparer = new ColumnsComparer();

    /**
     * @return The default failure handler
     * @since 2.4
     */
    protected FailureHandler getDefaultFailureHandler() {
        return getDefaultFailureHandler(null);
    }

    /**
     * @return The default failure handler
     * @since 2.4
     */
    protected FailureHandler getDefaultFailureHandler(final Column[] additionalColumnInfo) {
        final DefaultFailureHandler failureHandler = new DefaultFailureHandler(additionalColumnInfo);
        if (junitFailureFactory != null) {
            failureHandler.setFailureFactory(junitFailureFactory);
        }
        return failureHandler;
    }

    /**
     * @return the JUnitFailureFactory if JUnit is on the classpath or
     *         <code>null</code> if JUnit is not on the classpath.
     */
    private FailureHandler getJUnitFailureFactory() {
        try {
            Class.forName("junit.framework.Assert");
            // JUnit available
            return new JUnitFailureFactory();
        } catch (final ClassNotFoundException e) {
            // JUnit not available on the classpath return null
            log.debug("JUnit does not seem to be on the classpath. " + e);
        }
        return null;
    }

    protected FailureHandler determineFailureHandler(final FailureHandler failureHandler) {
        final FailureHandler validFailureHandler;

        if (failureHandler == null) {
            log.debug("FailureHandler is null. Using default implementation");
            validFailureHandler = getDefaultFailureHandler();
        } else {
            validFailureHandler = failureHandler;
        }

        return validFailureHandler;
    }

    protected boolean compareRowCounts(final ITable expectedTable, final ITable actualTable,
            final FailureHandler failureHandler, final String expectedTableName) throws Error {

        int actualRowsCount;
        try {
            actualRowsCount = actualTable.getRowCount();
        } catch (final UnsupportedOperationException exception) {
            return false;
        }
        final int expectedRowsCount = expectedTable.getRowCount();

        if (expectedRowsCount != actualRowsCount) {
            final String msg = "row count (table=" + expectedTableName + ")";
            failureHandler.handleFailure(msg, String.valueOf(expectedRowsCount), String.valueOf(actualRowsCount));
        }

        return actualRowsCount == 0;
    }

    protected void compareTableCounts(final String[] expectedNames, final String[] actualNames,
            final FailureHandler failureHandler) throws Error {
        if (expectedNames.length != actualNames.length) {
            failureHandler.handleFailure("table count", String.valueOf(expectedNames.length),
                    String.valueOf(actualNames.length));
        }
    }

    protected void compareTableNames(final String[] expectedNames, final String[] actualNames,
            final FailureHandler failureHandler) throws Error {
        for (int i = 0; i < expectedNames.length; i++) {
            if (!actualNames[i].equals(expectedNames[i])) {
                failureHandler.handleFailure("tables", Arrays.asList(expectedNames).toString(),
                        Arrays.asList(actualNames).toString());
            }
        }
    }

    protected String[] getSortedTableNames(final IDataSet dataSet) throws DataSetException {
        log.debug("getSortedTableNames(dataSet={}) - start", dataSet);

        final String[] names = dataSet.getTableNames();
        if (!dataSet.isCaseSensitiveTableNames()) {
            for (int i = 0; i < names.length; i++) {
                names[i] = names[i].toUpperCase();
            }
        }
        Arrays.sort(names);
        return names;
    }

    /**
     * Asserts the two specified {@link IDataSet}s comparing their columns using the
     * specified columnValueComparers or defaultValueComparer and handles failures
     * using the specified failureHandler. This method ignores the table names, the
     * columns order, the columns data type, and which columns are composing the
     * primary keys.
     *
     * @param expectedDataSet           {@link IDataSet} containing all expected
     *                                  results.
     * @param actualDataSet             {@link IDataSet} containing all actual
     *                                  results.
     * @param failureHandler            The failure handler used if the assert fails
     *                                  because of a data mismatch. Provides some
     *                                  additional information that may be useful to
     *                                  quickly identify the rows for which the
     *                                  mismatch occurred (for example by printing
     *                                  an additional primary key column). Can be
     *                                  <code>null</code>.
     * @param defaultValueComparer      {@link ValueComparer} to use with column
     *                                  value comparisons when the column name for
     *                                  the table is not in the
     *                                  tableColumnValueComparers {@link Map}. Can
     *                                  be <code>null</code> and will default to
     *                                  {@link #getDefaultValueComparer()}.
     * @param tableColumnValueComparers {@link Map} of {@link ValueComparer}s to use
     *                                  for specific tables and columns. Key is
     *                                  table name, value is {@link Map} of column
     *                                  name in the table to {@link ValueComparer}s.
     *                                  Can be <code>null</code> and will default to
     *                                  using
     *                                  {@link #getDefaultColumnValueComparerMapForTable(String)}
     *                                  or, if that is empty, defaultValueComparer
     *                                  for all columns in all tables.
     * @throws DatabaseUnitException
     */
    public void assertWithValueComparer(final IDataSet expectedDataSet, final IDataSet actualDataSet,
            final FailureHandler failureHandler, TableColumnValueComparerSource tableColumnValueComparerSource)
            throws DataSetException, Error, DatabaseUnitException {
        // do not continue if same instance
        if (expectedDataSet == actualDataSet) {
            log.debug("The given datasets reference the same object." + " Skipping comparisons.");
            return;
        }

        final FailureHandler validFailureHandler = determineFailureHandler(failureHandler);

        final String[] expectedNames = getSortedTableNames(expectedDataSet);
        final String[] actualNames = getSortedTableNames(actualDataSet);

        compareTableCounts(expectedNames, actualNames, validFailureHandler);

        // table names in no specific order
        compareTableNames(expectedNames, actualNames, validFailureHandler);

        compareTables(expectedDataSet, actualDataSet, expectedNames, validFailureHandler,
                tableColumnValueComparerSource);
    }

    protected void compareTables(final IDataSet expectedDataSet, final IDataSet actualDataSet,
            final String[] expectedNames, final FailureHandler failureHandler,
            TableColumnValueComparerSource tableColumnValueComparerSource)
            throws DataSetException, Error, DatabaseUnitException {

        for (final String tableName : expectedNames) {
            final ITable expectedTable = expectedDataSet.getTable(tableName);
            final ITable actualTable = actualDataSet.getTable(tableName);

            MessageBuilder messageBuilder;
            if (failureHandler instanceof DefaultFailureHandler) {
                messageBuilder = ((DefaultFailureHandler) failureHandler).getMessageBuilder();
            } else {
                messageBuilder = new MessageBuilder(null);
            }

            if (tableColumnValueComparerSource == null) {
                tableColumnValueComparerSource = new TableColumnValueComparerSource();
            }
            ColumnValueComparerSource columnValueComparerSource = tableColumnValueComparerSource
                    .getColumnValueComparerSource(tableName);

            assertWithValueComparer2(expectedTable, actualTable, failureHandler, messageBuilder,
                    columnValueComparerSource);
        }
    }

    /**
     * Asserts the two specified {@link ITable}s comparing their columns using the
     * specified columnValueComparers or defaultValueComparer and handles failures
     * using the specified failureHandler. This method ignores the table names, the
     * columns order, the columns data type, and which columns are composing the
     * primary keys.
     *
     * @param expectedTable        {@link ITable} containing all expected results.
     * @param actualTable          {@link ITable} containing all actual results.
     * @param failureHandler       The failure handler used if the assert fails
     *                             because of a data mismatch. Provides some
     *                             additional information that may be useful to
     *                             quickly identify the rows for which the mismatch
     *                             occurred (for example by printing an additional
     *                             primary key column). Can be <code>null</code>.
     * @param defaultValueComparer {@link ValueComparer} to use with column value
     *                             comparisons when the column name for the table is
     *                             not in the columnValueComparers {@link Map}. Can
     *                             be <code>null</code> and will default to
     *                             {@link #getDefaultValueComparer()}.
     * @param columnValueComparers {@link Map} of {@link ValueComparer}s to use for
     *                             specific columns. Key is column name in the
     *                             table, value is {@link ValueComparer} to use in
     *                             comparing expected to actual column values. Can
     *                             be <code>null</code> and will default to using
     *                             {@link #getDefaultColumnValueComparerMapForTable(String)}
     *                             or, if that is empty, defaultValueComparer for
     *                             all columns in the table.
     * @param excludedColumn TODO
     * @throws DatabaseUnitException
     */

    public void assertWithValueComparer(final ITable expectedTable, final ITable actualTable,
            final FailureHandler failureHandler, Predicate<Column> excludedColumn, MessageBuilder messageBuilder,
            ColumnValueComparerSource columnValueComparerSource) throws Error, DataSetException, DatabaseUnitException {
        // Do not continue if same instance
        if (expectedTable == actualTable) {
            log.debug("The given tables reference the same object." + " Skipping comparisons.");
            return;
        }

        FailureHandler validFailureHandler;
        if (failureHandler == null) {
            log.debug("FailureHandler is null. Using default implementation");
            validFailureHandler = getDefaultFailureHandler();
        } else {
            validFailureHandler = failureHandler;
        }

        final ITableMetaData expectedMetaData = expectedTable.getTableMetaData();
        final ITableMetaData actualMetaData = actualTable.getTableMetaData();
        final String expectedTableName = expectedMetaData.getTableName();

        final boolean isTablesEmpty = compareRowCounts(expectedTable, actualTable, validFailureHandler,
                expectedTableName);
        if (isTablesEmpty) {
            return;
        }

        // Verify columns
        columnComparer.compareColumns(expectedMetaData, actualMetaData, excludedColumn, validFailureHandler);

        // Put the columns into the same order
        final Column[] expectedColumns = filter(Columns.getSortedColumns(expectedMetaData), excludedColumn);
        final Column[] actualColumns = filter(Columns.getSortedColumns(actualMetaData), excludedColumn);
        final ComparisonColumn[] comparisonCols = new ComparisonColumn[expectedColumns.length];

        for (int j = 0; j < expectedColumns.length; j++) {
            final Column expectedColumn = expectedColumns[j];
            final Column actualColumn = actualColumns[j];
            comparisonCols[j] = new ComparisonColumn(expectedTableName, expectedColumn, actualColumn,
                    validFailureHandler);
        }

        // Get the datatypes to be used for comparing the sorted columns

        // Finally compare the data

        compareRows(expectedTable, actualTable, columnValueComparerSource, comparisonCols, validFailureHandler,
                messageBuilder);
    }

    /**
     * @param sortedColumns
     * @param excludedColumn
     * @return
     */
    private Column[] filter(Column[] sortedColumns, Predicate<Column> excludedColumn) {
        List<Column> result = new ArrayList<>(asList(sortedColumns));
        result.removeIf(excludedColumn);
        return result.toArray(new Column[result.size()]);
    }

    void compareRows(final ITable expectedTable, final ITable actualTable,
            ColumnValueComparerSource columnValueComparerSource, final ComparisonColumn[] comparisonCols,
            final FailureHandler failureHandler, MessageBuilder messageBuilder)
            throws DataSetException, DatabaseUnitException {
        // iterate over all rows
        for (int rowNum = 0; rowNum < expectedTable.getRowCount(); rowNum++) {
            // iterate over all columns of the current row
            final int columnCount = comparisonCols.length;
            for (int columnNum = 0; columnNum < columnCount; columnNum++) {
                final ComparisonColumn compareColumn = comparisonCols[columnNum];
                final String columnName = compareColumn.getColumnName();
                final DataType dataType = compareColumn.getDataType();

                compireColumnValue(expectedTable, actualTable, rowNum, columnName, columnValueComparerSource,
                        failureHandler, messageBuilder, dataType);
            }
        }
    }

    void compireColumnValue(final ITable expectedTable, final ITable actualTable, final int rowNum,
            final String columnName, ColumnValueComparerSource columnValueComparerSource,
            final FailureHandler failureHandler, MessageBuilder messageBuilder, final DataType dataType)
            throws DataSetException, DatabaseUnitException {
        final Object expectedValue = expectedTable.getValue(rowNum, columnName);
        final Object actualValue = actualTable.getValue(rowNum, columnName);

        // Compare the values
        final ValueComparer valueComparer = columnValueComparerSource.selectValueComparer(columnName);

        final String failMessage = valueComparer.compare(dataType, expectedValue, actualValue);

        if (failMessage != null) {
            final String msg = messageBuilder.buildMessage(expectedTable, actualTable, rowNum, columnName, failMessage);
            failureHandler.handleFailure(msg, String.valueOf(expectedValue), String.valueOf(actualValue));
        }
    }

    public void assertWithValueComparerWithTableDefaults(final ITable expectedTable, final ITable actualTable,
            final FailureHandler failureHandler, Predicate<Column> excludedColumn, MessageBuilder messageBuilder,
            ColumnValueComparerSource columnValueComparerSource, ValueComparerDefaults valueComparerDefaults)
            throws Error, DataSetException, DatabaseUnitException {

        final String expectedTableName = expectedTable.getTableMetaData().getTableName();

        ColumnValueComparerSource columnValueComparerSourceWithDefaults = columnValueComparerSource
                .applyTableDefaults(expectedTableName, valueComparerDefaults);

        assertWithValueComparer(expectedTable, actualTable, failureHandler, excludedColumn, messageBuilder,
                columnValueComparerSourceWithDefaults);
    }

    public void assertWithValueComparer2(final ITable expectedTable, final ITable actualTable,
            final FailureHandler failureHandler, MessageBuilder messageBuilder,
            ColumnValueComparerSource columnValueComparerSource) throws Error, DataSetException, DatabaseUnitException {
        assertWithValueComparer(expectedTable, actualTable, failureHandler, (Predicate<Column>) c -> false,
                messageBuilder, columnValueComparerSource);
    }

    public void assertWithValueComparer3(final ITable expectedTable, final ITable actualTable,
            final FailureHandler failureHandler, MessageBuilder messageBuilder,
            final ColumnValueComparerSource columnValueComparerSource)
            throws Error, DataSetException, DatabaseUnitException {
        assertWithValueComparerWithTableDefaults(expectedTable, actualTable, failureHandler,
                (Predicate<Column>) c -> false, messageBuilder, columnValueComparerSource, valueComparerDefaults);
    }
}
