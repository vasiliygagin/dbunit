package org.dbunit.ext.postgresql;

import static org.junit.Assert.assertArrayEquals;

import java.io.StringReader;
import java.sql.Statement;
import java.sql.Types;

import org.dbunit.DatabaseEnvironmentLoader;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.ITableMetaData;
import org.dbunit.dataset.ReplacementDataSet;
import org.dbunit.dataset.xml.FlatXmlDataSetBuilder;
import org.dbunit.operation.DatabaseOperation;
import org.xml.sax.InputSource;

import junit.framework.TestCase;

public class PostgresSQLOidIT extends TestCase {
    private IDatabaseConnection _connection;

 // @formatter:off
    private static final String xmlData ="<?xml version=\"1.0\"?>" +
            "<dataset>" +
            "<T2 DATA=\"[NULL]\" />" +
            "<T2 DATA=\"\\[text UTF-8](Anything)\" />" +
            "</dataset>";
 // @formatter:on

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        // Load active postgreSQL profile and connection from Maven pom.xml.
        _connection = DatabaseEnvironmentLoader.getInstance(null).getConnection();
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        if (_connection != null) {
            _connection.close();
            _connection = null;
        }
    }

    public void testOk() {
    }

    public void xtestOidDataType() throws Exception {
        final String testTable = "t2";
        assertNotNull("didn't get a connection", _connection);
        _connection.getDatabaseConfig().setDataTypeFactory(new PostgresqlDataTypeFactory());
        Statement stat = _connection.getConnection().createStatement();
        // DELETE SQL OID tables
        stat.execute("DROP TABLE IF EXISTS " + testTable + ";");

        // Create SQL OID tables
        stat.execute("CREATE TABLE " + testTable + "(DATA OID);");
        stat.close();

        try {
            ReplacementDataSet dataSet = new ReplacementDataSet(
                    new FlatXmlDataSetBuilder().build(new InputSource(new StringReader(xmlData))));
            dataSet.addReplacementObject("[NULL]", null);
            dataSet.setStrictReplacement(true);

            IDataSet ids;
            ids = _connection.createDataSet();
            ITableMetaData itmd = ids.getTableMetaData(testTable);
            Column[] cols = itmd.getColumns();
            ids = _connection.createDataSet();
            for (Column col : cols) {
                assertEquals(Types.BIGINT, col.getDataType().getSqlType());
                assertEquals("oid", col.getSqlTypeName());
            }

            DatabaseOperation.CLEAN_INSERT.execute(_connection, dataSet);
            ids = _connection.createDataSet();
            ITable it = ids.getTable(testTable);
            assertNull(it.getValue(0, "DATA"));
            assertArrayEquals("\\[text UTF-8](Anything)".getBytes(), (byte[]) it.getValue(1, "DATA"));
        } catch (Exception e) {
            assertEquals("DatabaseOperation.CLEAN_INSERT... no exception", "" + e);
        }
    }
}
