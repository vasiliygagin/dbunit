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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.net.MalformedURLException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.dbunit.DatabaseUnitException;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.database.QueryDataSet;
import org.dbunit.database.metadata.MetadataManager;
import org.dbunit.dataset.CachedDataSet;
import org.dbunit.ext.hsqldb.HsqldbDatabaseConfig;
import org.dbunit.internal.connections.DriverManagerConnectionSource;
import org.dbunit.junit.internal.GlobalContext;
import org.dbunit.operation.DatabaseOperation;
import org.dbunit.testutil.TestUtils;
import org.dbunit.util.FileHelper;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class CsvURLProducerTest {

    private String driverClass;
    private String url;
    private String user;
    private String password;
    private IDatabaseConnection connection;
    private static final String THE_DIRECTORY = "csv/orders";

    private void produceAndInsertToDatabase() throws DatabaseUnitException, SQLException, MalformedURLException {
        CsvURLProducer producer = new CsvURLProducer(TestUtils.getFile(THE_DIRECTORY).toURL(),
                CsvDataSet.TABLE_ORDERING_FILE);
        CachedDataSet consumer = new CachedDataSet();
        producer.produce(consumer);
        DatabaseOperation operation = DatabaseOperation.INSERT;
        operation.execute(connection, consumer);
    }

    @Test
    public void testExportTaskWithCsvFormat() throws MalformedURLException, DatabaseUnitException, SQLException {
        produceAndInsertToDatabase();

        final String fromAnt = "target/csv/from-ant";
        final File dir = new File(fromAnt);
        FileHelper.deleteDirectory(dir);

        try {
            QueryDataSet queryDataSet = new QueryDataSet(getConnection());
            queryDataSet.addTable("orders", "select * from orders");
            queryDataSet.addTable("orders_row", "select * from orders_row");

            CsvDataSetWriter writer = new CsvDataSetWriter(dir);
            writer.write(queryDataSet);

            final File ordersFile = new File(fromAnt + "/orders.csv");
            assertTrue("file '" + ordersFile.getAbsolutePath() + "' does not exists", ordersFile.exists());
            final File ordersRowFile = new File(fromAnt + "/orders_row.csv");
            assertTrue("file " + ordersRowFile + " does not exists", ordersRowFile.exists());
        } finally {
            FileHelper.deleteDirectory(dir);
        }
    }

    private IDatabaseConnection getConnection() throws SQLException, DatabaseUnitException {
        HsqldbDatabaseConfig config = new HsqldbDatabaseConfig();
        DriverManagerConnectionSource driverManagerConnectionSource = GlobalContext.getIt()
                .getDriverManagerConnectionSource();
        Connection connection2 = driverManagerConnectionSource.fetchConnection(Object.class.getName(), url, user,
                password);
        MetadataManager metadataManager = new MetadataManager(connection2, config, null, null);
        return new DatabaseConnection(connection2, config, metadataManager);
    }

    @Before
    public void setUp() throws Exception {
        Properties properties = new Properties();
        final FileInputStream inStream = TestUtils.getFileInputStream("csv/cvs-tests.properties");
        properties.load(inStream);
        inStream.close();
        driverClass = properties.getProperty("cvs-tests.driver.class");
        url = properties.getProperty("cvs-tests.url");
        user = properties.getProperty("cvs-tests.user");
        password = properties.getProperty("cvs-tests.password");
        assertFalse("".equals(driverClass));
        assertFalse("".equals(url));
        assertFalse("".equals(user));
        Class.forName(driverClass);
        connection = getConnection();
        Statement statement = connection.getConnection().createStatement();
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
    }

    @After
    public void tearDown() throws Exception {
        executeSql();
        connection.close();
    }

    private void executeSql() throws SQLException {

        try (final Statement statement = connection.getConnection().createStatement();) {
            statement.execute("SHUTDOWN IMMEDIATELY");
        }
    }
}
