package org.dbunit.ext.postgresql;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.StringReader;
import java.sql.Statement;

import org.dbunit.AbstractDatabaseTest;
import org.dbunit.PostgresqlEnvironment;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.ReplacementDataSet;
import org.dbunit.dataset.xml.FlatXmlDataSetBuilder;
import org.dbunit.operation.DatabaseOperation;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.xml.sax.InputSource;

/**
 * Testcase for Postgresql to check SQL CREATE DOMAIN with FlatXmlDataSetBuilder
 * to insert a dataset with SQL Domains (user-def-types).
 *
 * @author Philipp S. (Unwissender2009)
 * @since Nov 23, 2009
 */
public class SQLHelperDomainPostgreSQLIT extends AbstractDatabaseTest {

    private IDatabaseConnection _connection;

    private static final String xmlData = "<?xml version=\"1.0\"?>" + "<dataset>" + "<T1 PK=\"1\" STATE=\"is_blabla\"/>"
            + "</dataset>";

    public SQLHelperDomainPostgreSQLIT() throws Exception {
    }

    @Override
    protected boolean checkEnvironment() {
        return environment instanceof PostgresqlEnvironment;
    }

    @Before
    public final void setUp() throws Exception {
        // Load active postgreSQL profile and connection from Maven pom.xml.
        _connection = database.getConnection();
    }

    @After
    public final void tearDown() throws Exception {
        if (_connection != null) {
            _connection.close();
            _connection = null;
        }
    }

    @Test
    public void testOk() {
    }

    public void xtestDomainDataTypes() throws Exception {

        assertNotNull("didn't get a connection", _connection);

        Statement stat = _connection.getConnection().createStatement();
        // DELETE SQL DOMAIN and Table with DOMAINS
        stat.execute("DROP TABLE  IF EXISTS T1;");
        stat.execute("DROP DOMAIN IF EXISTS MYSTATE;");
        stat.execute("DROP DOMAIN IF EXISTS MYPK;");

        // Create SQL DOMAIN and Table with DOMAINS
        stat.execute("CREATE DOMAIN MYSTATE AS VARCHAR(20) DEFAULT 'is_Valid';");
        stat.execute("CREATE DOMAIN MYPK AS INTEGER DEFAULT 0;");
        stat.execute("CREATE TABLE T1 (PK MYPK,STATE MYSTATE,PRIMARY KEY (PK));");
        stat.close();

        try {
            ReplacementDataSet dataSet = new ReplacementDataSet(
                    new FlatXmlDataSetBuilder().build(new InputSource(new StringReader(xmlData))));
            dataSet.addReplacementObject("[NULL]", null);
            dataSet.setStrictReplacement(true);

            // THE TEST -> hopefully with no exception!!!
            DatabaseOperation.CLEAN_INSERT.execute(_connection, dataSet);

            // Check Types.
            for (int i = 0; i < _connection.createDataSet().getTableMetaData("T1").getColumns().length; i++) {
                Column c = _connection.createDataSet().getTableMetaData("T1").getColumns()[i];

                if (c.getSqlTypeName().compareTo("mypk") == 0) {
                    assertEquals(java.sql.Types.INTEGER, c.getDataType().getSqlType());
                } else if (c.getSqlTypeName().compareTo("mystate") == 0) {
                    assertEquals(java.sql.Types.VARCHAR, c.getDataType().getSqlType());
                } else {
                    assertTrue(false);
                }
            }
        } catch (Exception e) {
            assertEquals("DatabaseOperation.CLEAN_INSERT... no exception", "" + e);
        }

    }
}
