/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.operation;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.dbunit.assertion.TestDataSet;
import org.dbunit.assertion.TestTable;
import org.dbunit.database.AbstractDatabaseConnection;
import org.dbunit.database.statement.IBatchStatement;
import org.dbunit.database.statement.IStatementFactory;
import org.dbunit.dataset.IDataSet;
import org.junit.Test;

import io.github.vasiliygagin.dbunit.jdbc.DatabaseConfig;

public class DeleteAllOperationTest {

    private DeleteAllOperation tested = new DeleteAllOperation();

    @Test
    public void testMockExecute() throws Exception {
        TestTable testTable1 = new TestTable("T1");
        TestTable testTable2 = new TestTable("T2");
        TestTable testTable3 = new TestTable("T1");
        TestDataSet testDataSet = new TestDataSet(testTable1, testTable2, testTable3);

        AbstractDatabaseConnection connection = mock(AbstractDatabaseConnection.class);
        IStatementFactory statementFactory = mock(IStatementFactory.class);
        IDataSet databaseDataSet = mock(IDataSet.class);

        DatabaseConfig databaseConfig = new DatabaseConfig();
        databaseConfig.setStatementFactory(statementFactory);

        when(connection.createDataSet()).thenReturn(databaseDataSet);
        when(connection.getDatabaseConfig()).thenReturn(databaseConfig);

        IBatchStatement statement = mock(IBatchStatement.class);
        when(statementFactory.createBatchStatement(connection)).thenReturn(statement);

        when(connection.correctTableName("T1")).thenReturn("S.T1");
        when(connection.correctTableName("T2")).thenReturn("S.T2");

        // execute operation
        tested.execute(connection, testDataSet);

        verify(statement).addBatch("delete from S.T1");
        verify(statement).addBatch("delete from S.T2");
        verify(statement).executeBatch();
        verify(statement).clearBatch();
        verify(statement).close();
    }
}
