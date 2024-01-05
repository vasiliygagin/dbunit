/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.util;

/**
 *
 */
public class QualifiedTableName2 {

    public final String schema;
    public final String table;

    public QualifiedTableName2(String schema, String table) {
        this.schema = schema;
        this.table = table;
    }

    public static QualifiedTableName2 parseFullTableName2(String fullTableName, String defaultSchema) {
        String schema;
        String table;

        // If there is a dot, then ignore default schema, parse schema and table out of full table name
        int firstDotIndex = fullTableName.indexOf(".");
        if (firstDotIndex != -1) {
            schema = fullTableName.substring(0, firstDotIndex);
            table = fullTableName.substring(firstDotIndex + 1);
        } else {
            // No schema name found in table
            table = fullTableName;
            // If the schema has not been found in the given table name
            // (that means there is no "MYSCHEMA.MYTABLE" but only a "MYTABLE")
            // then set the schema to the given default schema
            schema = defaultSchema;
        }
        return new QualifiedTableName2(schema, table);
    }
}
