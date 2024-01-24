package org.dbunit.operation;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.dbunit.database.AbstractDatabaseConnection;
import org.dbunit.database.DatabaseDataSet;
import org.dbunit.database.statement.MockBatchStatement;
import org.dbunit.database.statement.MockStatementFactory;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.DefaultTable;
import org.dbunit.dataset.DefaultTableIterator;
import org.dbunit.dataset.DefaultTableMetaData;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.datatype.DataType;
import org.junit.Test;

import io.github.vasiliygagin.dbunit.jdbc.DatabaseConfig;

public class UpdateOperationTest {

    ////////////////////////////////////////////////////////////////////////////
    //

    public UpdateOperationTest() throws Exception {
    }

    ////////////////////////////////////////////////////////////////////////////
    //
    @Test
    public void testMockExecute() throws Exception {
        String schemaName = "schema";
        String tableName = "table";
        String[] expected = { "update schema.table set c2 = 1234, c3 = 'false' where c4 = 0 and c1 = 'toto'",
                "update schema.table set c2 = 123.45, c3 = NULL where c4 = 0 and c1 = 'qwerty'", };

        Column[] columns = { new Column("c1", DataType.VARCHAR), new Column("c2", DataType.NUMERIC),
                new Column("c3", DataType.VARCHAR), new Column("c4", DataType.NUMERIC), };
        String[] primaryKeys = { "c4", "c1" };
        DefaultTable table = new DefaultTable(new DefaultTableMetaData(tableName, columns, primaryKeys));
        table.addRow(new Object[] { "toto", "1234", "false", "0" });
        table.addRow(new Object[] { "qwerty", new Double("123.45"), null, "0" });

        DatabaseDataSet dataSet = mock(DatabaseDataSet.class);
        when(dataSet.iterator()).thenReturn(new DefaultTableIterator(new ITable[] { table }));
        when(dataSet.getTableMetaData(tableName)).thenReturn(table.getTableMetaData());

        // setup mock objects
        MockBatchStatement statement = new MockBatchStatement();
        statement.addExpectedBatchStrings(expected);
        statement.setExpectedExecuteBatchCalls(1);
        statement.setExpectedClearBatchCalls(1);
        statement.setExpectedCloseCalls(1);

        MockStatementFactory factory = new MockStatementFactory();
        factory.setExpectedCreatePreparedStatementCalls(1);
        factory.setupStatement(statement);

        DatabaseConfig databaseConfig = new DatabaseConfig();
        databaseConfig.setStatementFactory(factory);
        AbstractDatabaseConnection connection = mock(AbstractDatabaseConnection.class);
        when(connection.getDatabaseConfig()).thenReturn(databaseConfig);
        when(connection.createDataSet()).thenReturn(dataSet);
        when(connection.getSchema()).thenReturn(schemaName);

        // execute operation
        new UpdateOperation().execute(connection, dataSet);

        statement.verify();
        factory.verify();
    }

    @Test
    public void testExecuteWithBlanksDisabledAndEmptyString() throws Exception {
        String schemaName = "schema";
        String tableName = "table";

        Column[] columns = { new Column("c3", DataType.VARCHAR), new Column("c4", DataType.NUMERIC), };
        String[] primaryKeys = { "c4" };
        DefaultTable table = new DefaultTable(new DefaultTableMetaData(tableName, columns, primaryKeys));
        table.addRow(new Object[] { "", "1" });

        DatabaseDataSet dataSet = mock(DatabaseDataSet.class);
        when(dataSet.iterator()).thenReturn(new DefaultTableIterator(new ITable[] { table }));
        when(dataSet.getTableMetaData(tableName)).thenReturn(table.getTableMetaData());

        // setup mock objects
        MockBatchStatement statement = new MockBatchStatement();
        statement.setExpectedExecuteBatchCalls(0);
        statement.setExpectedClearBatchCalls(0);
        statement.setExpectedCloseCalls(1);

        MockStatementFactory factory = new MockStatementFactory();
        factory.setExpectedCreatePreparedStatementCalls(1);
        factory.setupStatement(statement);

        DatabaseConfig databaseConfig = new DatabaseConfig();
        databaseConfig.setStatementFactory(factory);
        AbstractDatabaseConnection connection = mock(AbstractDatabaseConnection.class);
        when(connection.getDatabaseConfig()).thenReturn(databaseConfig);
        when(connection.createDataSet()).thenReturn(dataSet);
        when(connection.getSchema()).thenReturn(schemaName);

        // execute operation
        connection.getDatabaseConfig().setAllowEmptyFields(false);
        try {
            new UpdateOperation().execute(connection, dataSet);
            fail("Update should not succedd");
        } catch (IllegalArgumentException e) {
            // ignore
        } finally {
            statement.verify();
            factory.verify();
        }
    }

    @Test
    public void testExecuteWithBlanksDisabledAndNonEmptyStrings() throws Exception {
        String schemaName = "schema";
        String tableName = "table";
        String[] expected = { String.format("update %s.%s set c3 = 'not-empty' where c4 = 1", schemaName, tableName),
                String.format("update %s.%s set c3 = NULL where c4 = 2", schemaName, tableName) };

        Column[] columns = { new Column("c3", DataType.VARCHAR), new Column("c4", DataType.NUMERIC), };
        String[] primaryKeys = { "c4" };
        DefaultTable table = new DefaultTable(new DefaultTableMetaData(tableName, columns, primaryKeys));
        table.addRow(new Object[] { "not-empty", "1" });
        table.addRow(new Object[] { null, "2" });

        DatabaseDataSet dataSet = mock(DatabaseDataSet.class);
        when(dataSet.iterator()).thenReturn(new DefaultTableIterator(new ITable[] { table }));
        when(dataSet.getTableMetaData(tableName)).thenReturn(table.getTableMetaData());

        // setup mock objects
        MockBatchStatement statement = new MockBatchStatement();
        statement.addExpectedBatchStrings(expected);
        statement.setExpectedExecuteBatchCalls(1);
        statement.setExpectedClearBatchCalls(1);
        statement.setExpectedCloseCalls(1);

        MockStatementFactory factory = new MockStatementFactory();
        factory.setExpectedCreatePreparedStatementCalls(1);
        factory.setupStatement(statement);

        DatabaseConfig databaseConfig = new DatabaseConfig();
        databaseConfig.setStatementFactory(factory);
        AbstractDatabaseConnection connection = mock(AbstractDatabaseConnection.class);
        when(connection.getDatabaseConfig()).thenReturn(databaseConfig);
        when(connection.createDataSet()).thenReturn(dataSet);
        when(connection.getSchema()).thenReturn(schemaName);

        // execute operation
        connection.getDatabaseConfig().setAllowEmptyFields(false);
        new UpdateOperation().execute(connection, dataSet);

        statement.verify();
        factory.verify();
    }

    @Test
    public void testExecuteWithBlanksAllowed() throws Exception {
        String schemaName = "schema";
        String tableName = "table";
        String[] expected = { String.format("update %s.%s set c3 = 'not-empty' where c4 = 1", schemaName, tableName),
                String.format("update %s.%s set c3 = NULL where c4 = 2", schemaName, tableName),
                String.format("update %s.%s set c3 = '' where c4 = 3", schemaName, tableName), };

        Column[] columns = { new Column("c3", DataType.VARCHAR), new Column("c4", DataType.NUMERIC), };
        String[] primaryKeys = { "c4" };
        DefaultTable table = new DefaultTable(new DefaultTableMetaData(tableName, columns, primaryKeys));
        table.addRow(new Object[] { "not-empty", "1" });
        table.addRow(new Object[] { null, "2" });
        table.addRow(new Object[] { "", "3" });

        DatabaseDataSet dataSet = mock(DatabaseDataSet.class);
        when(dataSet.iterator()).thenReturn(new DefaultTableIterator(new ITable[] { table }));
        when(dataSet.getTableMetaData(tableName)).thenReturn(table.getTableMetaData());

        // setup mock objects
        MockBatchStatement statement = new MockBatchStatement();
        statement.addExpectedBatchStrings(expected);
        statement.setExpectedExecuteBatchCalls(1);
        statement.setExpectedClearBatchCalls(1);
        statement.setExpectedCloseCalls(1);

        MockStatementFactory factory = new MockStatementFactory();
        factory.setExpectedCreatePreparedStatementCalls(1);
        factory.setupStatement(statement);

        DatabaseConfig databaseConfig = new DatabaseConfig();
        databaseConfig.setStatementFactory(factory);
        AbstractDatabaseConnection connection = mock(AbstractDatabaseConnection.class);
        when(connection.getDatabaseConfig()).thenReturn(databaseConfig);
        when(connection.createDataSet()).thenReturn(dataSet);
        when(connection.getSchema()).thenReturn(schemaName);

        // execute operation
        connection.getDatabaseConfig().setAllowEmptyFields(true);
        new UpdateOperation().execute(connection, dataSet);

        statement.verify();
        factory.verify();
    }

    @Test
    public void testExecuteWithEscapedName() throws Exception {
        String schemaName = "schema";
        String tableName = "table";
        String[] expected = {
                "update [schema].[table] set [c2] = 1234, [c3] = 'false' where [c4] = 0 and [c1] = 'toto'",
                "update [schema].[table] set [c2] = 123.45, [c3] = NULL where [c4] = 0 and [c1] = 'qwerty'", };

        Column[] columns = { new Column("c1", DataType.VARCHAR), new Column("c2", DataType.NUMERIC),
                new Column("c3", DataType.VARCHAR), new Column("c4", DataType.NUMERIC), };
        String[] primaryKeys = { "c4", "c1" };
        DefaultTable table = new DefaultTable(new DefaultTableMetaData(tableName, columns, primaryKeys));
        table.addRow(new Object[] { "toto", "1234", "false", "0" });
        table.addRow(new Object[] { "qwerty", new Double("123.45"), null, "0" });

        DatabaseDataSet dataSet = mock(DatabaseDataSet.class);
        when(dataSet.iterator()).thenReturn(new DefaultTableIterator(new ITable[] { table }));
        when(dataSet.getTableMetaData(tableName)).thenReturn(table.getTableMetaData());

        // setup mock objects
        MockBatchStatement statement = new MockBatchStatement();
        statement.addExpectedBatchStrings(expected);
        statement.setExpectedExecuteBatchCalls(1);
        statement.setExpectedClearBatchCalls(1);
        statement.setExpectedCloseCalls(1);

        MockStatementFactory factory = new MockStatementFactory();
        factory.setExpectedCreatePreparedStatementCalls(1);
        factory.setupStatement(statement);

        DatabaseConfig databaseConfig = new DatabaseConfig();
        databaseConfig.setStatementFactory(factory);
        AbstractDatabaseConnection connection = mock(AbstractDatabaseConnection.class);
        when(connection.getDatabaseConfig()).thenReturn(databaseConfig);
        when(connection.createDataSet()).thenReturn(dataSet);
        when(connection.getSchema()).thenReturn(schemaName);

        // execute operation
        connection.getDatabaseConfig().setEscapePattern("[?]");
        new UpdateOperation().execute(connection, dataSet);

        statement.verify();
        factory.verify();
    }

    @Test
    public void testExecuteWithEmptyTable() throws Exception {
        Column[] columns = { new Column("c1", DataType.VARCHAR) };
        ITable table = new DefaultTable(new DefaultTableMetaData("name", columns, columns));

        DatabaseDataSet dataSet = mock(DatabaseDataSet.class);
        when(dataSet.iterator()).thenReturn(new DefaultTableIterator(new ITable[] { table }));
        when(dataSet.getTableMetaData("name")).thenReturn(table.getTableMetaData());

        // setup mock objects
        MockStatementFactory factory = new MockStatementFactory();
        factory.setExpectedCreatePreparedStatementCalls(0);

        DatabaseConfig databaseConfig = new DatabaseConfig();
        databaseConfig.setStatementFactory(factory);
        AbstractDatabaseConnection connection = mock(AbstractDatabaseConnection.class);
        when(connection.getDatabaseConfig()).thenReturn(databaseConfig);
        when(connection.createDataSet()).thenReturn(dataSet);

        // execute operation
        new UpdateOperation().execute(connection, dataSet);

        factory.verify();
    }
}
