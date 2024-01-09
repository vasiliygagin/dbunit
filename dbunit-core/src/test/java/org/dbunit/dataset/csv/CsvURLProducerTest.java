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
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.dbunit.AbstractDatabaseTest;
import org.dbunit.DatabaseUnitException;
import org.dbunit.dataset.CachedDataSet;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.stream.IDataSetProducer;
import org.dbunit.dataset.stream.StreamingDataSet;
import org.dbunit.operation.DatabaseOperation;
import org.dbunit.testutil.TestUtils;
import org.junit.Before;
import org.junit.Test;

public class CsvURLProducerTest extends AbstractDatabaseTest {

    private static final int ORDERS_ROWS_NUMBER = 5;
    private static final int ORDERS_ROW_ROWS_NUMBER = 3;
    private static final String THE_DIRECTORY = "csv/orders";

    public CsvURLProducerTest() throws Exception {
    }

    @Before
    public final void setUp() throws Exception {

        Statement statement = database.getJdbcConnection().createStatement();
        statement.execute("CREATE TABLE ORDERS (ID INTEGER, DESCRIPTION VARCHAR(100))");
        statement.execute("CREATE TABLE ORDERS_ROW (ID INTEGER, DESCRIPTION VARCHAR(100), QUANTITY INTEGER)");
        statement.close();
    }

    @Test
    public void testProduceFromFolder() throws DataSetException, MalformedURLException {
        CsvURLProducer producer = new CsvURLProducer(TestUtils.getFile(THE_DIRECTORY).toURI().toURL(),
                CsvDataSet.TABLE_ORDERING_FILE);
        doTestWithProducer(producer);
    }

    @Test
    public void testProduceFromJar() throws DataSetException, IOException {
        File file = TestUtils.getFile(THE_DIRECTORY + "/orders.jar");
        URL jarFile = new URL("jar:" + file.toURI().toURL() + "!/");
        CsvURLProducer producer = new CsvURLProducer(jarFile, CsvDataSet.TABLE_ORDERING_FILE);
        doTestWithProducer(producer);
    }

    private void doTestWithProducer(CsvURLProducer producer) throws DataSetException {
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
    public void testProduceAndInsertFromFolder() throws MalformedURLException, DatabaseUnitException, SQLException {
        produceAndInsertToDatabase();
        Statement statement = database.getJdbcConnection().createStatement();
        ResultSet resultSet = statement.executeQuery("select count(*) from orders");
        resultSet.next();
        int count = resultSet.getInt(1);
        assertEquals(ORDERS_ROWS_NUMBER, count);
        resultSet.close();
        statement.close();
    }

    private void produceAndInsertToDatabase() throws DatabaseUnitException, SQLException, MalformedURLException {
        CsvURLProducer producer = new CsvURLProducer(TestUtils.getFile(THE_DIRECTORY).toURI().toURL(),
                CsvDataSet.TABLE_ORDERING_FILE);
        CachedDataSet consumer = new CachedDataSet();
        producer.produce(consumer);
        DatabaseOperation.INSERT.execute(database.getConnection(), consumer);
    }

    @Test
    public void testInsertOperationWithCsvFormat() throws SQLException, DatabaseUnitException {
        try {
            IDataSetProducer producer = new CsvProducer(TestUtils.getFile(THE_DIRECTORY));
            IDataSet dataset = new StreamingDataSet(producer);
            DatabaseOperation.INSERT.execute(database.getConnection(), dataset);
        } catch (SQLException e) {
            throw new DatabaseUnitException(e);
        }
        Statement statement = database.getJdbcConnection().createStatement();
        ResultSet resultSet = statement.executeQuery("select count(*) from orders");
        resultSet.next();
        final int count = resultSet.getInt(1);
        assertEquals("wrong number of row in orders table", ORDERS_ROWS_NUMBER, count);
        resultSet.close();
        statement.close();
    }
}
