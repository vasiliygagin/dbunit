package org.dbunit.operation;

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
import org.dbunit.dataset.datatype.DataType;
import org.junit.Test;

import io.github.vasiliygagin.dbunit.jdbc.DatabaseConfig;

public class DeleteOperationTest {

    private DeleteOperation tested = new DeleteOperation();

    @Test
    public void testExecute() throws Exception {
        String schemaName = "schema";
        String tableName1 = "table1";
        String tableName2 = "table2";
        String[] expected = { "delete from schema.table2 where c2 = 1234 and c1 = 'toto'",
                "delete from schema.table2 where c2 = 123.45 and c1 = 'qwerty'",
                "delete from schema.table1 where c2 = 1234 and c1 = 'toto'",
                "delete from schema.table1 where c2 = 123.45 and c1 = 'qwerty'", };

        Column[] columns = { new Column("c1", DataType.VARCHAR), new Column("c2", DataType.NUMERIC),
                new Column("c3", DataType.BOOLEAN), };
        String[] primaryKeys = { "c2", "c1" };

        DefaultTable table1 = new DefaultTable(new DefaultTableMetaData(tableName1, columns, primaryKeys));
        table1.addRow(new Object[] { "qwerty", new Double("123.45"), "true" });
        table1.addRow(new Object[] { "toto", "1234", Boolean.FALSE });
        DefaultTable table2 = new DefaultTable(new DefaultTableMetaData(tableName2, columns, primaryKeys));
        table2.addTableRows(table1);
        DatabaseDataSet dataSet = mock(DatabaseDataSet.class); // new DefaultDataSet(table1, table2);
        when(dataSet.reverseIterator()).thenReturn(new DefaultTableIterator(new ITable[] { table2, table1 }));
        when(dataSet.getTableMetaData(tableName1)).thenReturn(table1.getTableMetaData());
        when(dataSet.getTableMetaData(tableName2)).thenReturn(table2.getTableMetaData());

        // setup mock objects
        MockBatchStatement statement = new MockBatchStatement();
        statement.addExpectedBatchStrings(expected);
        statement.setExpectedExecuteBatchCalls(2);
        statement.setExpectedClearBatchCalls(2);
        statement.setExpectedCloseCalls(2);

        MockStatementFactory factory = new MockStatementFactory();
        factory.setExpectedCreatePreparedStatementCalls(2);
        factory.setupStatement(statement);

        DatabaseConfig databaseConfig = new DatabaseConfig();
        databaseConfig.setStatementFactory(factory);
        AbstractDatabaseConnection connection = mock(AbstractDatabaseConnection.class);
        when(connection.getDatabaseConfig()).thenReturn(databaseConfig);
        when(connection.createDataSet()).thenReturn(dataSet);
        when(connection.getSchema()).thenReturn(schemaName);

        // execute operation
        tested.execute(connection, dataSet);

        statement.verify();
        factory.verify();
    }

    @Test
    public void testExecuteWithEscapedNames() throws Exception {
        String schemaName = "schema";
        String tableName = "table";
        String[] expected = { "delete from [schema].[table] where [c2] = 123.45 and [c1] = 'qwerty'",
                "delete from [schema].[table] where [c2] = 1234 and [c1] = 'toto'", };

        Column[] columns = { new Column("c1", DataType.VARCHAR), new Column("c2", DataType.NUMERIC),
                new Column("c3", DataType.BOOLEAN), };
        String[] primaryKeys = { "c2", "c1" };

        DefaultTable table = new DefaultTable(new DefaultTableMetaData(tableName, columns, primaryKeys));
        table.addRow(new Object[] { "toto", "1234", Boolean.FALSE });
        table.addRow(new Object[] { "qwerty", new Double("123.45"), "true" });
        DatabaseDataSet dataSet = mock(DatabaseDataSet.class); // new DefaultDataSet(table1, table2);
        when(dataSet.reverseIterator()).thenReturn(new DefaultTableIterator(new ITable[] { table }));
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
        new DeleteOperation().execute(connection, dataSet);

        statement.verify();
        factory.verify();
    }

    @Test
    public void testExecuteWithEmptyTable() throws Exception {
        Column[] columns = { new Column("c1", DataType.VARCHAR) };
        ITable table = new DefaultTable(new DefaultTableMetaData("name", columns, columns));
        IDataSet dataSet = new DefaultDataSet(table);

        // setup mock objects
        MockStatementFactory factory = new MockStatementFactory();
        factory.setExpectedCreatePreparedStatementCalls(0);

        DatabaseConfig databaseConfig = new DatabaseConfig();
        databaseConfig.setStatementFactory(factory);
        AbstractDatabaseConnection connection = mock(AbstractDatabaseConnection.class);
        when(connection.getDatabaseConfig()).thenReturn(databaseConfig);

        // execute operation
        new DeleteOperation().execute(connection, dataSet);

        factory.verify();
    }
}
