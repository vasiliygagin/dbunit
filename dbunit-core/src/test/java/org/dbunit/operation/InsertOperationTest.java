package org.dbunit.operation;

import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.dbunit.database.AbstractDatabaseConnection;
import org.dbunit.database.DatabaseDataSet;
import org.dbunit.database.statement.MockBatchStatement;
import org.dbunit.database.statement.MockStatementFactory;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.DefaultDataSet;
import org.dbunit.dataset.DefaultTable;
import org.dbunit.dataset.DefaultTableIterator;
import org.dbunit.dataset.DefaultTableMetaData;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.NoSuchColumnException;
import org.dbunit.dataset.datatype.DataType;
import org.junit.Test;

import io.github.vasiliygagin.dbunit.jdbc.DatabaseConfig;

public class InsertOperationTest {

    public InsertOperationTest() throws Exception {
    }

    @Test
    public void testMockExecute() throws Exception {
        String schemaName = "schema";
        String tableName = "table";
        String[] expected = { "insert into schema.table (c1, c2, c3) values ('toto', 1234, 'false')",
                "insert into schema.table (c1, c2, c3) values ('qwerty', 123.45, 'true')", };

        // setup table
        Column[] columns = { new Column("c1", DataType.VARCHAR), new Column("c2", DataType.NUMERIC),
                new Column("c3", DataType.BOOLEAN), };
        DefaultTable table = new DefaultTable(tableName, columns);
        table.addRow(new Object[] { "toto", "1234", Boolean.FALSE });
        table.addRow(new Object[] { "qwerty", new Double("123.45"), "true" });
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
        new InsertOperation().execute(connection, dataSet);

        statement.verify();
        factory.verify();
    }

    @Test
    public void testExecuteWithBlanksDisabledAndEmptyString() throws Exception {
        String schemaName = "schema";
        String tableName = "table";

        Column[] columns = { new Column("c3", DataType.VARCHAR), new Column("c4", DataType.NUMERIC), };
        DefaultTable table = new DefaultTable(tableName, columns);
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
            new InsertOperation().execute(connection, dataSet);
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
        String[] expected = {
                String.format("insert into %s.%s (c3, c4) values ('not-empty', 1)", schemaName, tableName),
                String.format("insert into %s.%s (c3, c4) values (NULL, 2)", schemaName, tableName) };

        Column[] columns = { new Column("c3", DataType.VARCHAR), new Column("c4", DataType.NUMERIC), };
        DefaultTable table = new DefaultTable(tableName, columns);
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
        new InsertOperation().execute(connection, dataSet);

        statement.verify();
        factory.verify();
    }

    @Test
    public void testExecuteWithBlanksAllowed() throws Exception {
        String schemaName = "schema";
        String tableName = "table";
        String[] expected = {
                String.format("insert into %s.%s (c3, c4) values ('not-empty', 1)", schemaName, tableName),
                String.format("insert into %s.%s (c3, c4) values (NULL, 2)", schemaName, tableName),
                String.format("insert into %s.%s (c3, c4) values ('', 3)", schemaName, tableName), };

        Column[] columns = { new Column("c3", DataType.VARCHAR), new Column("c4", DataType.NUMERIC), };
        DefaultTable table = new DefaultTable(tableName, columns);
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
        new InsertOperation().execute(connection, dataSet);

        statement.verify();
        factory.verify();
    }

    @Test
    public void testExecuteUnknownColumn() throws Exception {
        String tableName = "table";

        // setup table
        Column[] columns = { new Column("column", DataType.VARCHAR), new Column("unknown", DataType.VARCHAR), };
        DefaultTable table = new DefaultTable(tableName, columns);
        table.addRow();
        table.setValue(0, columns[0].getColumnName(), null);
        table.setValue(0, columns[0].getColumnName(), "value");
        IDataSet insertDataset = new DefaultDataSet(table);

//        IDataSet databaseDataSet = new DefaultDataSet(
        DefaultTable wrongTable = new DefaultTable(tableName, new Column[] { new Column("column", DataType.VARCHAR), });
        // );
        DatabaseDataSet databaseDataSet = mock(DatabaseDataSet.class);
        when(databaseDataSet.iterator()).thenReturn(new DefaultTableIterator(new ITable[] { table }));
        when(databaseDataSet.getTableMetaData(tableName)).thenReturn(wrongTable.getTableMetaData());

        // setup mock objects
        MockBatchStatement statement = new MockBatchStatement();
        statement.setExpectedExecuteBatchCalls(0);
        statement.setExpectedClearBatchCalls(0);
        statement.setExpectedCloseCalls(0);

        MockStatementFactory factory = new MockStatementFactory();
        factory.setExpectedCreatePreparedStatementCalls(0);
        factory.setupStatement(statement);

        DatabaseConfig databaseConfig = new DatabaseConfig();
        databaseConfig.setStatementFactory(factory);
        AbstractDatabaseConnection connection = mock(AbstractDatabaseConnection.class);
        when(connection.getDatabaseConfig()).thenReturn(databaseConfig);
        when(connection.createDataSet()).thenReturn(databaseDataSet);

        // execute operation
        try {
            new InsertOperation().execute(connection, insertDataset);
            fail("Should not be here!");
        } catch (NoSuchColumnException e) {

        }

        statement.verify();
        factory.verify();
    }

    @Test
    public void testExecuteIgnoreNone() throws Exception {
        String schemaName = "schema";
        String tableName = "table";
        String[] expected = { "insert into schema.table (c1, c2, c3) values ('toto', 1234, 'false')",
                "insert into schema.table (c2, c3) values (123.45, 'true')",
                "insert into schema.table (c1, c2, c3) values ('qwerty1', 1, 'true')",
                "insert into schema.table (c1, c2, c3) values ('qwerty2', 2, 'false')",
                "insert into schema.table (c3) values ('false')", };

        // setup table
        Column[] columns = { new Column("c1", DataType.VARCHAR), new Column("c2", DataType.NUMERIC),
                new Column("c3", DataType.BOOLEAN), };
        DefaultTable table = new DefaultTable(tableName, columns);
        table.addRow(new Object[] { "toto", "1234", Boolean.FALSE });
        table.addRow(new Object[] { ITable.NO_VALUE, new Double("123.45"), "true" });
        table.addRow(new Object[] { "qwerty1", "1", Boolean.TRUE });
        table.addRow(new Object[] { "qwerty2", "2", Boolean.FALSE });
        table.addRow(new Object[] { ITable.NO_VALUE, ITable.NO_VALUE, Boolean.FALSE });
        DatabaseDataSet dataSet = mock(DatabaseDataSet.class);
        when(dataSet.iterator()).thenReturn(new DefaultTableIterator(new ITable[] { table }));
        when(dataSet.getTableMetaData(tableName)).thenReturn(table.getTableMetaData());

        // setup mock objects
        MockBatchStatement statement = new MockBatchStatement();
        statement.addExpectedBatchStrings(expected);
        statement.setExpectedExecuteBatchCalls(4);
        statement.setExpectedClearBatchCalls(4);
        statement.setExpectedCloseCalls(4);

        MockStatementFactory factory = new MockStatementFactory();
        factory.setExpectedCreatePreparedStatementCalls(4);
        factory.setupStatement(statement);

        DatabaseConfig databaseConfig = new DatabaseConfig();
        databaseConfig.setStatementFactory(factory);
        AbstractDatabaseConnection connection = mock(AbstractDatabaseConnection.class);
        when(connection.getDatabaseConfig()).thenReturn(databaseConfig);
        when(connection.createDataSet()).thenReturn(dataSet);
        when(connection.getSchema()).thenReturn(schemaName);

        // execute operation
        new InsertOperation().execute(connection, dataSet);

        statement.verify();
        factory.verify();
    }

//    public void testExecuteNullAsNone() throws Exception
//    {
//        String schemaName = "schema";
//        String tableName = "table";
//        String[] expected = {
//            "insert into schema.table (c1, c2, c3) values ('toto', 1234, 'false')",
//            "insert into schema.table (c2, c3) values (123.45, 'true')",
//            "insert into schema.table (c1, c2, c3) values ('qwerty1', 1, 'true')",
//            "insert into schema.table (c1, c2, c3) values ('qwerty2', 2, 'false')",
//            "insert into schema.table (c3) values ('false')",
//        };
//
//        // setup table
//        List valueList = new ArrayList();
//        valueList.add(new Object[]{"toto", "1234", Boolean.FALSE});
//        valueList.add(new Object[]{null, new Double("123.45"), "true"});
//        valueList.add(new Object[]{"qwerty1", "1", Boolean.TRUE});
//        valueList.add(new Object[]{"qwerty2", "2", Boolean.FALSE});
//        valueList.add(new Object[]{null, null, Boolean.FALSE});
//        Column[] columns = new Column[]{
//            new Column("c1", DataType.VARCHAR),
//            new Column("c2", DataType.NUMERIC),
//            new Column("c3", DataType.BOOLEAN),
//        };
//        DefaultTable table = new DefaultTable(tableName, columns, valueList);
//        IDataSet dataSet = new DefaultDataSet(table);
//
//        // setup mock objects
//        MockBatchStatement statement = new MockBatchStatement();
//        statement.addExpectedBatchStrings(expected);
//        statement.setExpectedExecuteBatchCalls(4);
//        statement.setExpectedClearBatchCalls(4);
//        statement.setExpectedCloseCalls(4);
//
//        MockStatementFactory factory = new MockStatementFactory();
//        factory.setExpectedCreatePreparedStatementCalls(4);
//        factory.setupStatement(statement);
//
//        MockDatabaseConnection connection = new MockDatabaseConnection();
//        connection.setupDataSet(dataSet);
//        connection.setupSchema(schemaName);
//        connection.setupStatementFactory(factory);
//        connection.setExpectedCloseCalls(0);
//        DatabaseConfig config = connection.getConfig();
//        config.setFeature(DatabaseConfig.FEATURE_NULL_AS_NONE, true);
//
//        // execute operation
//        new InsertOperation().execute(connection, dataSet);
//
//        statement.verify();
//        factory.verify();
//        connection.verify();
//    }

    @Test
    public void testExecuteWithEscapedNames() throws Exception {
        String schemaName = "schema";
        String tableName = "table";
        String[] expected = { "insert into 'schema'.'table' ('c1', 'c2', 'c3') values ('toto', 1234, 'false')",
                "insert into 'schema'.'table' ('c1', 'c2', 'c3') values ('qwerty', 123.45, 'true')", };

        // setup table
        Column[] columns = { new Column("c1", DataType.VARCHAR), new Column("c2", DataType.NUMERIC),
                new Column("c3", DataType.BOOLEAN), };
        DefaultTable table = new DefaultTable(tableName, columns);
        table.addRow(new Object[] { "toto", "1234", Boolean.FALSE });
        table.addRow(new Object[] { "qwerty", new Double("123.45"), "true" });
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
        connection.getDatabaseConfig().setEscapePattern("'?'");
        new InsertOperation().execute(connection, dataSet);

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
        new InsertOperation().execute(connection, dataSet);

        factory.verify();
    }

    @Test
    public void testDefaultValues() throws Exception {
        String schemaName = "schema";
        String tableName = "table";
        String[] expected = { "insert into schema.table (c1, c3, c4) values (NULL, NULL, NULL)" };

        // setup table
        Column[] columns = { new Column("c1", DataType.NUMERIC, Column.NO_NULLS), // Disallow null, no
                                                                                  // default
                new Column("c2", DataType.NUMERIC, DataType.NUMERIC.toString(), Column.NO_NULLS, "2"), // Disallow null,
                                                                                                       // default
                new Column("c3", DataType.NUMERIC, Column.NULLABLE), // Allow null, no default
                new Column("c4", DataType.NUMERIC, DataType.NUMERIC.toString(), Column.NULLABLE, "4"), // Allow null,
                                                                                                       // default
        };
        DefaultTable table = new DefaultTable(tableName, columns);
        table.addRow(new Object[] { null, null, null, null });
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
        new InsertOperation().execute(connection, dataSet);

        statement.verify();
        factory.verify();
    }
}
