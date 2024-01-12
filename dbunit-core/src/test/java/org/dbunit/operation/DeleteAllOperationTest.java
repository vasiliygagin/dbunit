/*
 *
 * The DbUnit Database Testing Framework
 * Copyright (C)2002-2004, DbUnit.org
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 */

package org.dbunit.operation;

import org.dbunit.database.MockDatabaseConnection;
import org.dbunit.database.statement.MockBatchStatement;
import org.dbunit.database.statement.MockStatementFactory;
import org.dbunit.dataset.DefaultDataSet;
import org.dbunit.dataset.DefaultTable;
import org.dbunit.dataset.IDataSet;
import org.junit.Test;

/**
 * @author Manuel Laflamme
 * @author Eric Pugh TODO Refactor all the references to
 *         AbstractDataSetTest.removeExtraTestTables() to something better.
 * @version $Revision$
 * @since Feb 18, 2002
 */
public class DeleteAllOperationTest {

    private DeleteAllOperation tested = new DeleteAllOperation();

    @Test
    public void testMockExecute() throws Exception {
        String schemaName = "schema";
        String tableName = "table";
        String expected = "delete from " + schemaName + "." + tableName;

        IDataSet dataSet = new DefaultDataSet(new DefaultTable(tableName));

        // setup mock objects
        MockBatchStatement statement = new MockBatchStatement();
        statement.addExpectedBatchString(expected);
        statement.setExpectedExecuteBatchCalls(1);
        statement.setExpectedClearBatchCalls(1);
        statement.setExpectedCloseCalls(1);

        MockStatementFactory factory = new MockStatementFactory();
        factory.setExpectedCreateStatementCalls(1);
        factory.setupStatement(statement);

        MockDatabaseConnection connection = new MockDatabaseConnection();
        connection.setupDataSet(dataSet);
        connection.setupSchema(schemaName);
        connection.setupStatementFactory(factory);
        connection.setExpectedCloseCalls(0);

        // execute operation
        tested.execute(connection, dataSet);

        statement.verify();
        factory.verify();
        connection.verify();
    }

    @Test
    public void testExecuteWithEscapedNames() throws Exception {
        String schemaName = "schema";
        String tableName = "table";
        String expected = "delete from " + "'" + schemaName + "'.'" + tableName + "'";

        IDataSet dataSet = new DefaultDataSet(new DefaultTable(tableName));

        // setup mock objects
        MockBatchStatement statement = new MockBatchStatement();
        statement.addExpectedBatchString(expected);
        statement.setExpectedExecuteBatchCalls(1);
        statement.setExpectedClearBatchCalls(1);
        statement.setExpectedCloseCalls(1);

        MockStatementFactory factory = new MockStatementFactory();
        factory.setExpectedCreateStatementCalls(1);
        factory.setupStatement(statement);

        MockDatabaseConnection connection = new MockDatabaseConnection();
        connection.setupDataSet(dataSet);
        connection.setupSchema(schemaName);
        connection.setupStatementFactory(factory);
        connection.setExpectedCloseCalls(0);

        // execute operation
        connection.getDatabaseConfig().setEscapePattern("'?'");
        new DeleteAllOperation().execute(connection, dataSet);

        statement.verify();
        factory.verify();
        connection.verify();
    }
}
