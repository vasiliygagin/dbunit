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

public class RefreshOperationTest {

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
        DatabaseOperation.REFRESH.execute(connection, dataSet);

        factory.verify();
    }

    @Test
    public void testExecuteUnknownColumn() throws Exception {
        String tableName = "table";

        // setup table
        Column[] columns = { new Column("unknown", DataType.VARCHAR), };
        DefaultTable table = new DefaultTable(tableName, columns);
        table.addRow();
        table.setValue(0, columns[0].getColumnName(), "value");
        IDataSet insertDataset = new DefaultDataSet(table);

        DefaultTable wrongTable = new DefaultTable(tableName, new Column[] { new Column("column", DataType.VARCHAR), });
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
            new RefreshOperation().execute(connection, insertDataset);
            fail("Should not be here!");
        } catch (NoSuchColumnException e) {

        }

        statement.verify();
        factory.verify();
    }
}
