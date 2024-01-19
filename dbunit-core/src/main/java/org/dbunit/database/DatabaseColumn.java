/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.database;

import org.dbunit.database.metadata.ColumnMetadata;
import org.dbunit.database.metadata.TableMetadata;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.datatype.DataTypeException;
import org.dbunit.dataset.datatype.IDataTypeFactory;

/**
 *
 */
public class DatabaseColumn extends Column {

    public final TableMetadata tableMetadata;
    public final ColumnMetadata columnMetadata;

    /**
     * @param tableMetadata
     * @param columnMetadata
     * @param dataTypeFactory
     * @throws DataTypeException
     */
    public DatabaseColumn(TableMetadata tableMetadata, ColumnMetadata columnMetadata, IDataTypeFactory dataTypeFactory)
            throws DataTypeException {
        super(columnMetadata.columnName,
                dataTypeFactory.createDataType(columnMetadata.sqlType, columnMetadata.sqlTypeName,
                        tableMetadata.tableName, columnMetadata.columnName),
                columnMetadata.sqlTypeName, Column.NULLABLE_UNKNOWN, null, null, Column.AutoIncrement.UNKNOWN);
        this.tableMetadata = tableMetadata;
        this.columnMetadata = columnMetadata;
    }
}
