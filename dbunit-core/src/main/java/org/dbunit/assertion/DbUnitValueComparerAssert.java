package org.dbunit.assertion;

import java.util.Map;

import org.dbunit.DatabaseUnitException;
import org.dbunit.assertion.comparer.value.ValueComparer;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;

/**
 * DbUnit assertions using {@link ValueComparer}s for the column comparisons.
 *
 * @author Jeff Jensen
 * @since 2.6.0
 */
public class DbUnitValueComparerAssert extends DbUnitAssertBase {

    /**
     * Asserts the two specified {@link IDataSet}s comparing their columns using the
     * default {@link ValueComparer} and handles failures using the default
     * {@link FailureHandler}. This method ignores the table names, the columns
     * order, the columns data type, and which columns are composing the primary
     * keys.
     *
     * @param expectedDataSet {@link IDataSet} containing all expected results.
     * @param actualDataSet   {@link IDataSet} containing all actual results.
     * @throws DatabaseUnitException
     */
    public void assertWithValueComparer(final IDataSet expectedDataSet, final IDataSet actualDataSet)
            throws DatabaseUnitException {
        TableColumnValueComparerSource tableColumnValueComparerSource = new TableColumnValueComparerSource();
        assertWithValueComparer(expectedDataSet, actualDataSet, tableColumnValueComparerSource);
    }

    /**
     * Asserts the two specified {@link IDataSet}s comparing their columns using the
     * specified defaultValueComparer and handles failures using the default
     * {@link FailureHandler}. This method ignores the table names, the columns
     * order, the columns data type, and which columns are composing the primary
     * keys.
     *
     * @param expectedDataSet      {@link IDataSet} containing all expected results.
     * @param actualDataSet        {@link IDataSet} containing all actual results.
     * @param defaultValueComparer {@link ValueComparer} to use with all column
     *                             value comparisons. Can be <code>null</code> and
     *                             will default to
     *                             {@link #getDefaultValueComparer()}.
     * @throws DatabaseUnitException
     */
    public void assertWithValueComparer(final IDataSet expectedDataSet, final IDataSet actualDataSet,
            final ValueComparer defaultValueComparer) throws DatabaseUnitException {
        TableColumnValueComparerSource tableColumnValueComparerSource = new TableColumnValueComparerSource(
                defaultValueComparer);

        assertWithValueComparer(expectedDataSet, actualDataSet, tableColumnValueComparerSource);
    }

    /**
     * Asserts the two specified {@link IDataSet}s comparing their columns using the
     * specified columnValueComparers or defaultValueComparer and handles failures
     * using the default {@link FailureHandler}. This method ignores the table
     * names, the columns order, the columns data type, and which columns are
     * composing the primary keys.
     *
     * @param expectedDataSet           {@link IDataSet} containing all expected
     *                                  results.
     * @param actualDataSet             {@link IDataSet} containing all actual
     *                                  results.
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
            TableColumnValueComparerSource tableColumnValueComparerSource)
            throws DataSetException, Error, DatabaseUnitException {
        final FailureHandler failureHandler = getDefaultFailureHandler();
        assertWithValueComparer(expectedDataSet, actualDataSet, failureHandler, tableColumnValueComparerSource);
    }

    /**
     * Asserts the two specified {@link ITable}s comparing their columns using the
     * default {@link ValueComparer} and handles failures using the default
     * {@link FailureHandler}. This method ignores the table names, the columns
     * order, the columns data type, and which columns are composing the primary
     * keys.
     *
     * @param expectedTable {@link ITable} containing all expected results.
     * @param actualTable   {@link ITable} containing all actual results.
     * @throws DatabaseUnitException
     */
    public void assertWithValueComparer(final ITable expectedTable, final ITable actualTable)
            throws DatabaseUnitException {

        final String tableName = expectedTable.getTableMetaData().getTableName();

        ColumnValueComparerSource columnValueComparerSource = valueComparerDefaults
                .getColumnValueComparerSource(tableName);

        assertWithValueComparer(expectedTable, actualTable, columnValueComparerSource);
    }

    /**
     * Asserts the two specified {@link ITable}s comparing their columns using the
     * specified defaultValueComparer and handles failures using the default
     * {@link FailureHandler}. This method ignores the table names, the columns
     * order, the columns data type, and which columns are composing the primary
     * keys.
     *
     * @param expectedTable        {@link ITable} containing all expected results.
     * @param actualTable          {@link ITable} containing all actual results.
     * @param defaultValueComparer {@link ValueComparer} to use with all column
     *                             value comparisons. Can be <code>null</code> and
     *                             will default to
     *                             {@link #getDefaultValueComparer()}.
     * @throws DatabaseUnitException
     */
    public void assertWithValueComparer(final ITable expectedTable, final ITable actualTable,
            final ValueComparer defaultValueComparer) throws DatabaseUnitException {
        final String tableName = expectedTable.getTableMetaData().getTableName();
        final Map<String, ValueComparer> columnValueComparers = valueComparerDefaults
                .getDefaultColumnValueComparerMapForTable(tableName);
        ColumnValueComparerSource columnValueComparerSource = new ColumnValueComparerSource(defaultValueComparer,
                columnValueComparers);

        assertWithValueComparer(expectedTable, actualTable, columnValueComparerSource);
    }

    /**
     * Asserts the two specified {@link ITable}s comparing their columns using the
     * specified columnValueComparers or defaultValueComparer and handles failures
     * using the default {@link FailureHandler}. This method ignores the table
     * names, the columns order, the columns data type, and which columns are
     * composing the primary keys.
     *
     * @param expectedTable        {@link ITable} containing all expected results.
     * @param actualTable          {@link ITable} containing all actual results.
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
     * @throws DatabaseUnitException
     */
    public void assertWithValueComparer(final ITable expectedTable, final ITable actualTable,
            final ColumnValueComparerSource columnValueComparerSource) throws DatabaseUnitException {
        final FailureHandler failureHandler = getDefaultFailureHandler();
        MessageBuilder messageBuilder;
        if (failureHandler instanceof DefaultFailureHandler) {
            messageBuilder = ((DefaultFailureHandler) failureHandler).getMessageBuilder();
        } else {
            messageBuilder = new MessageBuilder(null);
        }

        assertWithValueComparer3(expectedTable, actualTable, failureHandler, messageBuilder, columnValueComparerSource);
    }

    /**
     * Asserts the two specified {@link ITable}s comparing their columns using the
     * specified columnValueComparers or defaultValueComparer and handles failures
     * using the default {@link FailureHandler}, using additionalColumnInfo, if
     * specified. This method ignores the table names, the columns order, the
     * columns data type, and which columns are composing the primary keys.
     *
     * @param expectedTable        {@link ITable} containing all expected results.
     * @param actualTable          {@link ITable} containing all actual results.
     * @param additionalColumnInfo The columns to be printed out if the assert fails
     *                             because of a data mismatch. Provides some
     *                             additional column values that may be useful to
     *                             quickly identify the columns for which the
     *                             mismatch occurred (for example a primary key
     *                             column). Can be <code>null</code>
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
     * @throws DatabaseUnitException
     */
    public void assertWithValueComparer(final ITable expectedTable, final ITable actualTable,
            final Column[] additionalColumnInfo, final ColumnValueComparerSource columnValueComparerSource)
            throws DatabaseUnitException {
        final FailureHandler failureHandler = getDefaultFailureHandler(additionalColumnInfo);

        MessageBuilder messageBuilder;
        if (failureHandler instanceof DefaultFailureHandler) {
            messageBuilder = ((DefaultFailureHandler) failureHandler).getMessageBuilder();
        } else {
            messageBuilder = new MessageBuilder(null);
        }

        assertWithValueComparer3(expectedTable, actualTable, failureHandler, messageBuilder, columnValueComparerSource);
    }
}
