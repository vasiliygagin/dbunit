/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.dataset;

import java.util.function.Predicate;

import org.dbunit.assertion.FailureHandler;

public class ColumnsComparer {

    public ColumnsComparer() {
    }

    public void compareColumns(final ITableMetaData expectedMetaData, final ITableMetaData actualMetaData,
            Predicate<Column> excludedColumn, final FailureHandler failureHandler) throws DataSetException, Error {
        final Column[] expectedColumns = expectedMetaData.getColumns();
        final Column[] actualColumns = actualMetaData.getColumns();

        if (hasDifference(expectedMetaData, actualMetaData, excludedColumn)) {
            int expectedcColumnsCount = expectedMetaData.getColumns().length;
            int actualColumnsCount = actualMetaData.getColumns().length;
            String expectedTableName = expectedMetaData.getTableName();

            String message;
            if (expectedcColumnsCount != actualColumnsCount) {
                message = "column count (table=" + expectedTableName + ", " + "expectedColCount="
                        + expectedcColumnsCount + ", actualColCount=" + actualColumnsCount + ")";
            } else {
                message = "column mismatch (table=" + expectedTableName + ")";
            }
            failureHandler.handleFailure(message, Columns.getColumnNamesAsString(expectedColumns),
                    Columns.getColumnNamesAsString(actualColumns));
        }
    }

    private boolean hasDifference(final ITableMetaData expectedMetaData, final ITableMetaData actualMetaData,
            Predicate<Column> excludedColumn) throws DataSetException {
        try {
            // Get the columns that are missing on the actual side (walk through actual
            // columns and look for them in the expected metadata)
            checkColumnsExist(expectedMetaData, actualMetaData.getColumns(), excludedColumn);
            // Get the columns that are missing on the expected side (walk through expected
            // columns and look for them in the actual metadata)
            checkColumnsExist(actualMetaData, expectedMetaData.getColumns(), excludedColumn);
        } catch (NoSuchColumnException e) {
            return true;
        }
        return false;
    }

    private static void checkColumnsExist(ITableMetaData metaDataToCheck, Column[] columnsToSearch,
            Predicate<Column> excludedColumn) throws DataSetException {

        for (Column column : columnsToSearch) {
            if (excludedColumn.test(column)) {
                continue;
            }
            metaDataToCheck.getColumnIndex(column.getColumnName()); // throws NoSuchColumnException if column is missing
        }
    }
}
