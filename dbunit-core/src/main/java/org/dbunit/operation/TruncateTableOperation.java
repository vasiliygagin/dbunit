/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.operation;

public class TruncateTableOperation extends DeleteAllOperation {

    @Override
    protected String buildDeleteSql(String tableName) {
        return "truncate table " + tableName;
    }
}
