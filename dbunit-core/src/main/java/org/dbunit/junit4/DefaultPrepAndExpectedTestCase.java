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
package org.dbunit.junit4;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.dbunit.Assertion;
import org.dbunit.DatabaseUnitException;
import org.dbunit.PrepAndExpectedTestCaseSteps;
import org.dbunit.VerifyTableDefinition;
import org.dbunit.assertion.ColumnValueComparerSource;
import org.dbunit.assertion.comparer.value.ValueComparer;
import org.dbunit.database.AbstractDatabaseConnection;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.FullyLoadedTable;
import org.dbunit.database.ResultSetTable;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.CompositeDataSet;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.DefaultDataSet;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.ITableMetaData;
import org.dbunit.dataset.SortedTable;
import org.dbunit.dataset.datatype.DataType;
import org.dbunit.dataset.filter.DefaultColumnFilter;
import org.dbunit.junit.DbUnitFacade;
import org.dbunit.operation.DatabaseOperation;
import org.dbunit.util.TableFormatter;
import org.dbunit.util.fileloader.DataFileLoader;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test case base class supporting prep data and expected data. Prep data is the
 * data needed for the test to run. Expected data is the data needed to compare
 * if the test ran successfully.
 *
 * @see org.dbunit.junit4.DefaultPrepAndExpectedTestCaseDiIT
 * @see org.dbunit.junit4.DefaultPrepAndExpectedTestCaseExtIT
 *
 * @author Jeff Jensen jeffjensen AT users.sourceforge.net
 * @author Last changed by: $Author$
 * @version $Revision$ $Date$
 * @since 2.4.8
 */
public class DefaultPrepAndExpectedTestCase implements PrepAndExpectedTestCase {

    private final Logger log = LoggerFactory.getLogger(DefaultPrepAndExpectedTestCase.class);

    public static final String TEST_ERROR_MSG = "DbUnit test error.";

    private DataFileLoader dataFileLoader;

    // ?DatabaseSetp
    private IDataSet prepDataSet = new DefaultDataSet();
    // ?ExpectedDatabase
    private IDataSet expectedDataSet = new DefaultDataSet();
    private VerifyTableDefinition[] verifyTableDefs = {};

    final TableFormatter tableFormatter = new TableFormatter();

    @Rule
    public final DbUnitFacade dbUnit = new DbUnitFacade();

    /** Create new instance. */
    public DefaultPrepAndExpectedTestCase() {
    }

    public void configureTest(final String[] prepDataFiles, final String[] expectedDataFiles)
            throws Exception, DataSetException {
        this.prepDataSet = makeCompositeDataSet(prepDataFiles, true);
        this.expectedDataSet = makeCompositeDataSet(expectedDataFiles, true);
    }

    public void configureVerify(final VerifyTableDefinition[] verifyTableDefinitions) {
        this.verifyTableDefs = verifyTableDefinitions;
    }

    @Override
    public Object runTest(final VerifyTableDefinition[] verifyTables, final String[] prepDataFiles,
            final String[] expectedDataFiles, final PrepAndExpectedTestCaseSteps testSteps) throws Exception {
        final Object result;

        try {
            configureTest(prepDataFiles, expectedDataFiles);
            log.info("runTest: running test steps");
            result = testSteps.run();
        } catch (final Throwable e) {
            log.error(TEST_ERROR_MSG, e);
            throw e;
        }

        configureVerify(verifyTables);
        return result;
    }

    @Before
    public final void setUpDatabaseTester() throws Exception {
        DatabaseConnection connection = dbUnit.getConnection();
        DatabaseOperation.CLEAN_INSERT.execute(connection, prepDataSet);
    }

    @After
    public final void tearDown() throws Exception {
        // parent tearDown() only cleans up prep data

    }

    @After
    public void verifyData() throws Exception {

        final AbstractDatabaseConnection connection = dbUnit.getConnection();

        try {
            final int tableDefsCount = verifyTableDefs.length;

            for (int i = 0; i < tableDefsCount; i++) {
                final VerifyTableDefinition td = verifyTableDefs[i];
                verifyData(connection, td);
            }
        } catch (final Exception e) {
            log.error("verifyData: Exception:", e);
            throw e;
        } finally {
            log.debug("verifyData: Verification done, closing connection");
            connection.close();
        }
    }

    protected void verifyData(final AbstractDatabaseConnection connection,
            final VerifyTableDefinition verifyTableDefinition) throws Exception {
        final String tableName = verifyTableDefinition.getTableName();
        log.info("verifyData: Verifying table '{}'", tableName);

        final String[] excludeColumns = verifyTableDefinition.getColumnExclusionFilters();
        final String[] includeColumns = verifyTableDefinition.getColumnInclusionFilters();
        ColumnValueComparerSource columnValueComparerSource = verifyTableDefinition.getColumnValueComparerSource();

        final ITable expectedTable = loadTableDataFromDataSet(tableName);
        final FullyLoadedTable actualTable = loadTableDataFromDatabase(tableName, connection);

        verifyData(expectedTable, actualTable, excludeColumns, includeColumns, columnValueComparerSource);
    }

    public ITable loadTableDataFromDataSet(final String tableName) throws DataSetException {
        ITable table = null;

        final String methodName = "loadTableDataFromDataSet";

        log.debug("{}: Loading table {} from expected dataset", methodName, tableName);
        try {
            table = expectedDataSet.getTable(tableName);
        } catch (final Exception e) {
            final String msg = methodName + ": Problem obtaining table '" + tableName + "' from expected dataset";
            log.error(msg, e);
            throw new DataSetException(msg, e);
        }
        return table;
    }

    public FullyLoadedTable loadTableDataFromDatabase(final String tableName,
            final AbstractDatabaseConnection connection) throws Exception {
        FullyLoadedTable table;

        final String methodName = "loadTableDataFromDatabase";

        try {
            ResultSetTable resultSetTable = connection.loadTableResultSet(tableName);
            table = new FullyLoadedTable(resultSetTable);
        } catch (final Exception e) {
            final String msg = methodName + ": Problem obtaining table '" + tableName + "' from database";
            log.error(msg, e);
            throw new DataSetException(msg, e);
        }
        return table;
    }

    /**
     * For the specified expected and actual tables (and excluding and including the
     * specified columns), verify the actual data is as expected.
     *
     * @param expectedTable        The expected table to compare the actual table
     *                             to.
     * @param actualTable          The actual table to compare to the expected
     *                             table.
     * @param excludeColumns       The column names to exclude from comparison. See
     *                             {@link org.dbunit.dataset.filter.DefaultColumnFilter#excludeColumn(String)}
     *                             .
     * @param includeColumns       The column names to only include in comparison.
     *                             See
     *                             {@link org.dbunit.dataset.filter.DefaultColumnFilter#includeColumn(String)}
     *                             .
     * @param defaultValueComparer {@link ValueComparer} to use with column value
     *                             comparisons when the column name for the table is
     *                             not in the columnValueComparers {@link Map}. Can
     *                             be <code>null</code> and will default.
     * @param columnValueComparers {@link Map} of {@link ValueComparer}s to use for
     *                             specific columns. Key is column name, value is
     *                             the {@link ValueComparer}. Can be
     *                             <code>null</code> and will default to
     *                             defaultValueComparer for all columns in all
     *                             tables.
     * @throws DatabaseUnitException
     */
    protected void verifyData(final ITable expectedTable, final ITable actualTable, final String[] excludeColumns,
            final String[] includeColumns, final ColumnValueComparerSource columnValueComparerSource)
            throws DatabaseUnitException {
        final String methodName = "verifyData";

        final ITableMetaData actualTableMetaData = actualTable.getTableMetaData();
        final ITableMetaData expectedTableMetaData = expectedTable.getTableMetaData();

        final Column[] actualTableColumns = actualTableMetaData.getColumns();
        final Column[] expectedTableColumns = makeExpectedTableColumns(actualTableColumns, expectedTableMetaData);

        log.debug("{}: Sorting expected table using all columns", methodName);
        final SortedTable expectedSortedTable = new SortedTable(expectedTable, expectedTableColumns, true);
        expectedSortedTable.setUseComparable(true);
        log.debug("{}: Sorted expected table={}", methodName, expectedSortedTable);

        log.debug("{}: Sorting actual table using all columns", methodName);
        final SortedTable actualSortedTable = new SortedTable(actualTable, actualTableColumns);
        actualSortedTable.setUseComparable(true);
        log.debug("{}: Sorted actual table={}", methodName, actualSortedTable);

        // Filter out the columns from the expected and actual results
        log.debug("{}: Applying column exclude and include filters to sorted expected table", methodName);
        final ITable expectedFilteredTable = applyColumnFilters(expectedSortedTable, excludeColumns, includeColumns);
        log.debug("{}: Applying column exclude and include filters to sorted actual table", methodName);
        final ITable actualFilteredTable = applyColumnFilters(actualSortedTable, excludeColumns, includeColumns);

        log.debug("{}: Creating additionalColumnInfo for expected table", methodName);
        final Column[] additionalColumnInfo = makeAdditionalColumnInfo(expectedTable, excludeColumns);
        log.debug("{}: additionalColumnInfo={}", methodName, additionalColumnInfo);

        logSortedTables(expectedSortedTable, actualSortedTable);

        log.debug("{}: Comparing expected table to actual table", methodName);
        compareData(expectedFilteredTable, actualFilteredTable, additionalColumnInfo, columnValueComparerSource);
    }

    /**
     * If expected column definitions exist and are {@link DataType.UNKNOWN}, make
     * them from actual table column definitions.
     *
     * @throws DataSetException
     */
    private Column[] makeExpectedTableColumns(final Column[] actualColumns, final ITableMetaData expectedTableMetaData)
            throws DataSetException {
        final Column[] expectedTableColumns;

        final Column[] expectedColumns = expectedTableMetaData.getColumns();
        if (expectedColumns.length > 0) {
            final DataType dataType = expectedColumns[0].getDataType();
            if (DataType.UNKNOWN.equals(dataType)) {
                // all column definitions probably unknown, use actual's
                expectedTableColumns = makeExpectedTableColumns(actualColumns, expectedColumns);
            } else {
                // all expected column definitions probably known, use them
                expectedTableColumns = expectedColumns;
            }
        } else {
            // no column definitions exist, so don't falsely add any
            expectedTableColumns = expectedColumns;
        }

        return expectedTableColumns;
    }

    /**
     * Make expected Column[] from actual table column definitions so expected data
     * comparisons use data types from database (and expected data columns handled
     * same as actual data in comparisons). Don't include columns from actual that
     * are not in expected.
     */
    private Column[] makeExpectedTableColumns(final Column[] actualColumns, final Column[] expectedColumns) {
        final Set<String> expectedColumnNames = Arrays.stream(expectedColumns).map(Column::getColumnName)
                .map(String::toLowerCase).collect(Collectors.toSet());

        final List<Column> expectedColumnsList = Arrays.stream(actualColumns)
                .filter(col -> expectedColumnNames.contains(col.getColumnName().toLowerCase()))
                .collect(Collectors.toList());
        return expectedColumnsList.toArray(new Column[expectedColumnsList.size()]);
    }

    private void logSortedTables(final SortedTable expectedSortedTable, final SortedTable actualSortedTable) {
        if (log.isTraceEnabled()) {
            logSortedTable("expectedSortedTable", expectedSortedTable);
            logSortedTable("actualSortedTable", actualSortedTable);
        }
    }

    private void logSortedTable(final String tableTypeName, final SortedTable table) {
        final String methodName = "logSortedTable:";
        final Column[] sortColumns = table.getSortColumns();
        log.trace("{} {} sortColumns={}", methodName, tableTypeName, sortColumns);
        try {
            final String tableContents = tableFormatter.format(table);
            log.trace("{} {} tableContents={}", methodName, tableTypeName, tableContents);
        } catch (final DataSetException e) {
            log.error("{} Error trying to log table={}", methodName, tableTypeName, e);
        }
    }

    /** Compare the tables, enables easy overriding. */
    protected void compareData(final ITable expectedTable, final ITable actualTable,
            final Column[] additionalColumnInfo, final ColumnValueComparerSource columnValueComparerSource)
            throws DatabaseUnitException {
        Assertion.assertWithValueComparer(expectedTable, actualTable, additionalColumnInfo, columnValueComparerSource);
    }

    /**
     * Don't add excluded columns to additionalColumnInfo as they are not found and
     * generate a not found message in the fail message.
     *
     * @param expectedTable  Not null.
     * @param excludeColumns Nullable.
     */
    protected Column[] makeAdditionalColumnInfo(final ITable expectedTable, final String[] excludeColumns)
            throws DataSetException {
        final Column[] allColumns = expectedTable.getTableMetaData().getColumns();

        return excludeColumns == null ? allColumns : makeAdditionalColumnInfo(excludeColumns, allColumns);
    }

    /**
     * Don't add excluded columns to additionalColumnInfo as they are not found and
     * generate a not found message in the fail message.
     *
     * @param expectedTable  Not null.
     * @param excludeColumns Not null.
     */
    protected Column[] makeAdditionalColumnInfo(final String[] excludeColumns, final Column[] allColumns) {
        final List<Column> keepColumnsList = new ArrayList<>();
        final List<String> excludeColumnsList = Arrays.asList(excludeColumns);

        for (final Column column : allColumns) {
            final String columnName = column.getColumnName();
            if (!excludeColumnsList.contains(columnName)) {
                keepColumnsList.add(column);
            }
        }

        return keepColumnsList.toArray(new Column[keepColumnsList.size()]);
    }

    /**
     * Make a <code>IDataSet</code> from the specified files.
     *
     * @param dataFiles                 Represents the array of dbUnit data files.
     * @param isCaseSensitiveTableNames true if case sensitive table names is on.
     * @return The composite dataset.
     * @throws DataSetException On dbUnit errors.
     */
    private final CompositeDataSet makeCompositeDataSet(final String[] dataFiles,
            final boolean isCaseSensitiveTableNames) throws DataSetException {
        if (dataFileLoader == null) {
            throw new IllegalStateException("dataFileLoader is null; must configure or set it first");
        }

        final int count = dataFiles.length;
        final IDataSet[] dataSet = new IDataSet[count];
        for (int i = 0; i < count; i++) {
            dataSet[i] = dataFileLoader.load(dataFiles[i]);
        }

        return new CompositeDataSet(dataSet, true, isCaseSensitiveTableNames);
    }

    /**
     * Apply the specified exclude and include column filters to the specified
     * table.
     *
     * @param table          The table to apply the filters to.
     * @param excludeColumns The exclude filters; use null or empty array to mean
     *                       exclude none.
     * @param includeColumns The include filters; use null to mean include all.
     * @return The filtered table.
     * @throws DataSetException
     */
    public ITable applyColumnFilters(final ITable table, final String[] excludeColumns, final String[] includeColumns)
            throws DataSetException {
        ITable filteredTable = table;

        if (table == null) {
            throw new IllegalArgumentException("table is null");
        }

        // note: dbunit interprets an empty inclusion filter array as one
        // not wanting to compare anything!
        if (includeColumns == null) {
            log.debug("applyColumnFilters: including columns=(all)");
        } else {
            log.debug("applyColumnFilters: including columns='{}'", new Object[] { includeColumns });
            filteredTable = DefaultColumnFilter.includedColumnsTable(filteredTable, includeColumns);
        }

        if (excludeColumns == null || excludeColumns.length == 0) {
            log.debug("applyColumnFilters: excluding columns=(none)");
        } else {
            log.debug("applyColumnFilters: excluding columns='{}'", new Object[] { excludeColumns });
            filteredTable = DefaultColumnFilter.excludedColumnsTable(filteredTable, excludeColumns);
        }

        return filteredTable;
    }

    /**
     * {@inheritDoc}
     */

    @Override
    public IDataSet getPrepDataset() {
        return prepDataSet;
    }

    /**
     * {@inheritDoc}
     */

    @Override
    public IDataSet getExpectedDataset() {
        return expectedDataSet;
    }

    /**
     * Get the dataFileLoader.
     *
     * @see {@link #dataFileLoader}.
     *
     * @return The dataFileLoader.
     */
    public DataFileLoader getDataFileLoader() {
        return dataFileLoader;
    }

    /**
     * Set the dataFileLoader.
     *
     * @see {@link #dataFileLoader}.
     *
     * @param dataFileLoader The dataFileLoader to set.
     */
    public void setDataFileLoader(final DataFileLoader dataFileLoader) {
        this.dataFileLoader = dataFileLoader;
    }

    /**
     * Set the prepDs.
     *
     * @see {@link #prepDataSet}.
     *
     * @param prepDataSet The prepDs to set.
     */
    public void setPrepDs(final IDataSet prepDataSet) {
        this.prepDataSet = prepDataSet;
    }

    /**
     * Set the expectedDs.
     *
     * @see {@link #expectedDataSet}.
     *
     * @param expectedDataSet The expectedDs to set.
     */
    public void setExpectedDs(final IDataSet expectedDataSet) {
        this.expectedDataSet = expectedDataSet;
    }

    /**
     * Get the tableDefs.
     *
     * @see {@link #verifyTableDefs}.
     *
     * @return The tableDefs.
     */
    public VerifyTableDefinition[] getVerifyTableDefs() {
        return verifyTableDefs;
    }
}
