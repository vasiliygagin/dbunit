package org.dbunit.operation;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.dbunit.database.AbstractDatabaseConnection;
import org.dbunit.database.DatabaseDataSet;
import org.dbunit.database.statement.MockBatchStatement;
import org.dbunit.database.statement.MockStatementFactory;
import org.dbunit.dataset.DefaultTable;
import org.dbunit.dataset.DefaultTableIterator;
import org.dbunit.dataset.ITable;
import org.junit.Test;

import io.github.vasiliygagin.dbunit.jdbc.DatabaseConfig;

public class TruncateTableOperationTest {

    private TruncateTableOperation tested = new TruncateTableOperation();

    @Test
    public void testMockExecute() throws Exception {
        String schemaName = "schema";
        String tableName = "table";
        String expected = "truncate table " + schemaName + "." + tableName;

        DefaultTable table = new DefaultTable(tableName);
        DatabaseDataSet dataSet = mock(DatabaseDataSet.class);
        when(dataSet.iterator()).thenReturn(new DefaultTableIterator(new ITable[] { table }));
        when(dataSet.getTableMetaData(tableName)).thenReturn(table.getTableMetaData());

        // setup mock objects
        MockBatchStatement statement = new MockBatchStatement();
        statement.addExpectedBatchString(expected);
        statement.setExpectedExecuteBatchCalls(1);
        statement.setExpectedClearBatchCalls(1);
        statement.setExpectedCloseCalls(1);

        MockStatementFactory factory = new MockStatementFactory();
        factory.setExpectedCreateStatementCalls(1);
        factory.setupStatement(statement);

        DatabaseConfig databaseConfig = new DatabaseConfig();
        databaseConfig.setStatementFactory(factory);
        AbstractDatabaseConnection connection = mock(AbstractDatabaseConnection.class);
        when(connection.getDatabaseConfig()).thenReturn(databaseConfig);
        when(connection.createDataSet()).thenReturn(dataSet);
        when(connection.getSchema()).thenReturn(schemaName);
        when(connection.correctTableName(tableName)).thenReturn(schemaName + "." + tableName);

        // execute operation
        tested.execute(connection, dataSet);

        statement.verify();
        factory.verify();
    }

    @Test
    public void testExecuteWithEscapedNames() throws Exception {
        String schemaName = "schema";
        String tableName = "table";
        String expected = "truncate table " + "'" + schemaName + "'.'" + tableName + "'";

        DefaultTable table = new DefaultTable(tableName);
        DatabaseDataSet dataSet = mock(DatabaseDataSet.class);
        when(dataSet.iterator()).thenReturn(new DefaultTableIterator(new ITable[] { table }));
        when(dataSet.getTableMetaData(tableName)).thenReturn(table.getTableMetaData());

        // setup mock objects
        MockBatchStatement statement = new MockBatchStatement();
        statement.addExpectedBatchString(expected);
        statement.setExpectedExecuteBatchCalls(1);
        statement.setExpectedClearBatchCalls(1);
        statement.setExpectedCloseCalls(1);

        MockStatementFactory factory = new MockStatementFactory();
        factory.setExpectedCreateStatementCalls(1);
        factory.setupStatement(statement);

        DatabaseConfig databaseConfig = new DatabaseConfig();
        databaseConfig.setStatementFactory(factory);
        AbstractDatabaseConnection connection = mock(AbstractDatabaseConnection.class);
        when(connection.getDatabaseConfig()).thenReturn(databaseConfig);
        when(connection.createDataSet()).thenReturn(dataSet);
        when(connection.getSchema()).thenReturn(schemaName);
        when(connection.correctTableName(tableName)).thenReturn("'schema'.'table'");

        // execute operation
        connection.getDatabaseConfig().setEscapePattern("'?'");
        tested.execute(connection, dataSet);

        statement.verify();
        factory.verify();
    }
}
