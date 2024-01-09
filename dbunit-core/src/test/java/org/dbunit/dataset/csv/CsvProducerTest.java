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

package org.dbunit.dataset.csv;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.dbunit.DatabaseUnitException;
import org.dbunit.DdlExecutor;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.CachedDataSet;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.stream.IDataSetProducer;
import org.dbunit.dataset.stream.StreamingDataSet;
import org.dbunit.ext.hsqldb.HsqldbDatabaseConfig;
import org.dbunit.internal.connections.DriverManagerConnectionsFactory;
import org.dbunit.operation.DatabaseOperation;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class CsvProducerTest {

    private Connection jdbcConnection;
    private IDatabaseConnection connection;
    private static final int ORDERS_ROWS_NUMBER = 5;
    private static final int ORDERS_ROW_ROWS_NUMBER = 3;
    private static final String THE_DIRECTORY = "src/test/resources/csv/orders";

    @Test
    public void testProduceFromFolder() throws DataSetException {
        CsvProducer producer = new CsvProducer(THE_DIRECTORY);
        CachedDataSet consumer = new CachedDataSet();
        // producer.setConsumer(new CsvDataSetWriter("src/csv/orders-out"));

        producer.produce(consumer);
        final ITable[] tables = consumer.getTables();
        assertEquals("expected 2 tables", 2, tables.length);

        final ITable orders = consumer.getTable("orders");
        assertNotNull("orders table not found", orders);
        assertEquals("wrong number of rows", ORDERS_ROWS_NUMBER, orders.getRowCount());
        assertEquals("wrong number of columns", 2, orders.getTableMetaData().getColumns().length);

        final ITable ordersRow = consumer.getTable("orders_row");
        assertNotNull("orders_row table not found", ordersRow);
        assertEquals("wrong number of rows", ORDERS_ROW_ROWS_NUMBER, ordersRow.getRowCount());
        assertEquals("wrong number of columns", ORDERS_ROW_ROWS_NUMBER,
                ordersRow.getTableMetaData().getColumns().length);
    }

    @Test
    public void testProduceAndInsertFromFolder() throws DatabaseUnitException, SQLException {
        produceAndInsertToDatabase();
        Statement statement = jdbcConnection.createStatement();
        ResultSet resultSet = statement.executeQuery("select count(*) from orders");
        resultSet.next();
        int count = resultSet.getInt(1);
        assertEquals(ORDERS_ROWS_NUMBER, count);
        resultSet.close();
        statement.close();
    }

    private void produceAndInsertToDatabase() throws DatabaseUnitException, SQLException {
        CsvProducer producer = new CsvProducer(THE_DIRECTORY);
        CachedDataSet consumer = new CachedDataSet();
        producer.produce(consumer);
        DatabaseOperation.INSERT.execute(connection, consumer);
    }

    @Test
    public void testInsertOperationWithCsvFormat() throws SQLException, DatabaseUnitException {
        try {
            IDataSetProducer producer = new CsvProducer(new File(THE_DIRECTORY));
            IDataSet dataset = new StreamingDataSet(producer);
            DatabaseOperation.INSERT.execute(connection, dataset);
        } catch (SQLException e) {
            throw new DatabaseUnitException(e);
        }
        Statement statement = jdbcConnection.createStatement();
        ResultSet resultSet = statement.executeQuery("select count(*) from orders");
        resultSet.next();
        final int count = resultSet.getInt(1);
        assertEquals("wrong number of row in orders table", ORDERS_ROWS_NUMBER, count);
        resultSet.close();
        statement.close();
    }

    @Before
    public final void setUp() throws Exception {
        jdbcConnection = DriverManagerConnectionsFactory.getIT().fetchConnection("org.hsqldb.jdbcDriver",
                "jdbc:hsqldb:file:target/csv/orders-db/orders", "sa", "");

        Statement statement = jdbcConnection.createStatement();
        try {
            statement.execute("DROP TABLE ORDERS");
            statement.execute("DROP TABLE ORDERS_ROW");
        } catch (Exception ignored) {
        }
        statement.execute("CREATE TABLE ORDERS (ID INTEGER, DESCRIPTION VARCHAR(100))");
        statement.execute("CREATE TABLE ORDERS_ROW (ID INTEGER, DESCRIPTION VARCHAR(100), QUANTITY INTEGER)");
        // statement.execute("delete from orders");
        // statement.execute("delete from orders_row");
        statement.close();

        HsqldbDatabaseConfig config = new HsqldbDatabaseConfig();
        connection = new DatabaseConnection(jdbcConnection, config);
    }

    @After
    public final void tearDown() throws Exception {
        DdlExecutor.executeSql(jdbcConnection, "DROP SCHEMA PUBLIC IF EXISTS CASCADE");
        DdlExecutor.executeSql(jdbcConnection, "DROP SCHEMA TEST_SCHEMA IF EXISTS CASCADE");
        DdlExecutor.executeSql(jdbcConnection, "SET SCHEMA PUBLIC");
        connection.close();
    }
}
