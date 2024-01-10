/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.assertion;

import org.dbunit.dataset.ColumnFilterTable;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.ITableMetaData;
import org.dbunit.dataset.NoSuchColumnException;

/**
 *
 */
public class MessageBuilder {

    String[] _additionalColumnInfo;

    public MessageBuilder() {
        _additionalColumnInfo = null;
    }

    /**
     * @param columnNames
     */
    public MessageBuilder(String[] _additionalColumnInfo) {
        this._additionalColumnInfo = _additionalColumnInfo;
    }

    public String buildMessage(final ITable expectedTable, final ITable actualTable, final int rowNum,
            final String columnName, final String failMessage) {

        final StringBuilder builder = new StringBuilder(200);

        addFailMessage(builder, failMessage);

        final String expectedTableName = expectedTable.getTableMetaData().getTableName();

        // example message:
        // "value (table=MYTAB, row=232, column=MYCOL, Additional row info:
        // (column=MyIdCol, expected=444, actual=555)): expected:<123> but
        // was:<1234>"
        builder.append("value (table=").append(expectedTableName);
        builder.append(", row=").append(rowNum);
        builder.append(", col=").append(columnName);

        final String additionalInfo = getAdditionalInfo(rowNum, expectedTable, actualTable);
        if (additionalInfo != null && !additionalInfo.trim().equals("")) {
            builder.append(", ").append(additionalInfo);
        }

        builder.append(")");

        return builder.toString();
    }

    public String getAdditionalInfo(final int rowNum, final ITable expectedTable, final ITable actualTable) {
        // add custom column values information for better identification of
        // mismatching rows
        // No columns specified
        if (_additionalColumnInfo == null || _additionalColumnInfo.length <= 0) {
            return null;
        }

        final StringBuilder sb = new StringBuilder();
        sb.append("Additional row info:");
        for (final String columnName1 : _additionalColumnInfo) {
            final Object expectedKeyValue = getColumnValue(expectedTable, rowNum, columnName1);
            final Object actualKeyValue = getColumnValue(actualTable, rowNum, columnName1);

            sb.append(" ('");
            sb.append(columnName1);
            sb.append("': expected=<");
            sb.append(expectedKeyValue);
            sb.append(">, actual=<");
            sb.append(actualKeyValue);
            sb.append(">)");
        }

        return sb.toString();
    }

    private void addFailMessage(final StringBuilder builder, final String failMessage) {
        final boolean isFailMessage = isFailMessage(failMessage);
        if (isFailMessage) {
            builder.append(failMessage).append(": ");
        }
    }

    boolean isFailMessage(final String failMessage) {
        return failMessage != null && !failMessage.isEmpty();
    }

    Object getColumnValue(final ITable table, final int rowIndex, final String columnName) {
        Object value = null;
        try {
            // Get the ITable object to be used for showing the column values
            // (needed in case of Filtered tables)
            final ITable tableForCol = getTableForColumn(table, columnName);
            value = tableForCol.getValue(rowIndex, columnName);
        } catch (final DataSetException e) {
            value = makeAdditionalColumnInfoErrorMessage(columnName, e);
        }
        return value;
    }

    String makeAdditionalColumnInfoErrorMessage(final String columnName, final DataSetException e) {
        final StringBuilder sb = new StringBuilder();
        sb.append("Exception creating more info for column '");
        sb.append(columnName);
        sb.append("': ");
        sb.append(e.getClass().getName());
        sb.append(": ");
        sb.append(e.getMessage());
        final String msg = sb.toString();

        DefaultFailureHandler.logger.warn(msg, e);

        return " (!!!!! " + msg + ")";
    }

    /**
     * @param table      The table which might be a decorated table
     * @param columnName The column name for which a table is searched
     * @return The table that as a column with the given name
     * @throws DataSetException If no table could be found having a column with the
     *                          given name
     */
    ITable getTableForColumn(final ITable table, final String columnName) throws DataSetException {
        final ITableMetaData tableMetaData = table.getTableMetaData();
        try {
            tableMetaData.getColumnIndex(columnName);
            // if the column index was resolved the table contains the given
            // column. So just use this table
            return table;
        } catch (final NoSuchColumnException e) {
            // If the column was not found check for filtered table
            if (table instanceof ColumnFilterTable) {
                final ITableMetaData originalMetaData = ((ColumnFilterTable) table).getOriginalMetaData();
                originalMetaData.getColumnIndex(columnName);
                // If we get here the column exists - return the table since it
                // is not filtered in the CompositeTable.
                return table;
            } else {
                // Column not available in the table - rethrow the exception
                throw e;
            }
        }
    }
}
