/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.database;

/**
 *
 */
public class ResultSetRow {

    private final DatabaseColumn[] columns;
    private final Object[] rowValues;

    /**
     * @param columns
     * @param rowValues
     */
    public ResultSetRow(DatabaseColumn[] columns, Object[] rowValues) {
        this.columns = columns;
        this.rowValues = rowValues;
    }
//
//    public Object getValue(int row, String column) throws DataSetException {
//        assertValidRowIndex(row);
//
//        return rowValues[getColumnIndex(column)];
//    }
//
//    private void assertValidRowIndex(int row) throws DataSetException {
//        assertValidRowIndex(row, getRowCount());
//    }
//
//    private void assertValidRowIndex(int row, int rowCount) throws DataSetException {
//        if (row < 0) {
//            throw new RowOutOfBoundsException(row + " < 0");
//        }
//
//        if (row >= rowCount) {
//            throw new RowOutOfBoundsException(row + " >= " + rowCount);
//        }
//    }
//
//    private int getColumnIndex(String columnName) throws DataSetException {
//        ITableMetaData metaData = getTableMetaData();
//        return metaData.getColumnIndex(columnName);
//    }

    public Object[] getRowValues() {
        return rowValues;
    }
}
