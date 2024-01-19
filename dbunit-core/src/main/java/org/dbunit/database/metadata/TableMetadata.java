/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.database.metadata;

public class TableMetadata {

    public final SchemaMetadata schemaMetadata;
    public final String tableName;
    public final String tableType;

    ColumnMetadata[] columns;

    public TableMetadata(SchemaMetadata schema, String tableName, String tableType) {
        this.schemaMetadata = schema;
        this.tableName = tableName;
        this.tableType = tableType;
    }

    public ColumnMetadata[] getColumns() {
        return columns;
    }
}
