/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.database.metadata;

/**
 *
 */
public class ColumnMetadata {

    public final String columnName;
    public final int sqlType;
    public final String sqlTypeName;

    /**
     * @param columnName
     * @param sqlType
     * @param sqlTypeName
     */
    public ColumnMetadata(String columnName, int sqlType, String sqlTypeName) {
        this.columnName = columnName;
        this.sqlType = sqlType;
        this.sqlTypeName = sqlTypeName;
    }
}
