/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.database;

import static org.junit.Assert.assertEquals;

import org.dbunit.dataset.Column;
import org.dbunit.dataset.DefaultTableMetaData;
import org.dbunit.dataset.ITableMetaData;
import org.dbunit.dataset.datatype.DataType;

/**
 *
 */
public class AbstractResultSetTableTest {

    public void testGetSelectStatement() throws Exception {
        String schemaName = "schema";
        String tableName = "table";
        Column[] columns = { new Column("c1", DataType.UNKNOWN), new Column("c2", DataType.UNKNOWN),
                new Column("c3", DataType.UNKNOWN), };
        String expected = "select c1, c2, c3 from schema.table";

        ITableMetaData metaData = new DefaultTableMetaData(tableName, columns);
        String sql = AbstractResultSetTable.getSelectStatement(schemaName, metaData, null);
        assertEquals("select statement", expected, sql);
    }

    public void testGetSelectStatementWithEscapedNames() throws Exception {
        String schemaName = "schema";
        String tableName = "table";
        Column[] columns = { new Column("c1", DataType.UNKNOWN), new Column("c2", DataType.UNKNOWN),
                new Column("c3", DataType.UNKNOWN), };
        String expected = "select 'c1', 'c2', 'c3' from 'schema'.'table'";

        ITableMetaData metaData = new DefaultTableMetaData(tableName, columns);
        String sql = AbstractResultSetTable.getSelectStatement(schemaName, metaData, "'?'");
        assertEquals("select statement", expected, sql);
    }

    public void testGetSelectStatementWithEscapedNamesAndOrderBy() throws Exception {
        String schemaName = "schema";
        String tableName = "table";
        Column[] columns = { new Column("c1", DataType.UNKNOWN), new Column("c2", DataType.UNKNOWN),
                new Column("c3", DataType.UNKNOWN), };
        String expected = "select 'c1', 'c2', 'c3' from 'schema'.'table' order by 'c1', 'c2'";

        String[] primaryKeys = { "c1", "c2" };

        ITableMetaData metaData = new DefaultTableMetaData(tableName, columns, primaryKeys);
        String sql = AbstractResultSetTable.getSelectStatement(schemaName, metaData, "'?'");
        assertEquals("select statement", expected, sql);
    }

    public void testGetSelectStatementWithPrimaryKeys() throws Exception {
        String schemaName = "schema";
        String tableName = "table";
        Column[] columns = { new Column("c1", DataType.UNKNOWN), new Column("c2", DataType.UNKNOWN),
                new Column("c3", DataType.UNKNOWN), };
        String expected = "select c1, c2, c3 from schema.table order by c1, c2, c3";

        ITableMetaData metaData = new DefaultTableMetaData(tableName, columns, columns);
        String sql = AbstractResultSetTable.getSelectStatement(schemaName, metaData, null);
        assertEquals("select statement", expected, sql);
    }
}
