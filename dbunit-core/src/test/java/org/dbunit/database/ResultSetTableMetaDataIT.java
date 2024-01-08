package org.dbunit.database;

import java.sql.Connection;

import org.dbunit.AbstractDatabaseIT;
import org.dbunit.DdlExecutor;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.Columns;
import org.dbunit.dataset.IDataSet;
import org.dbunit.internal.connections.DriverManagerConnectionsFactory;
import org.dbunit.testutil.TestUtils;

/**
 * @author gommma (gommma AT users.sourceforge.net)
 * @author Last changed by: $Author$
 * @version $Revision$ $Date$
 * @since 2.4.0
 */
public class ResultSetTableMetaDataIT extends AbstractDatabaseIT {

    public ResultSetTableMetaDataIT(String s) {
        super(s);
    }

    protected IDataSet createDataSet() throws Exception {
        return customizedConnection.createDataSet();
    }

    /**
     * Tests the pattern-like column retrieval from the database. DbUnit should not
     * interpret any table names as regex patterns.
     *
     * @throws Exception
     */
    public void testGetColumnsForTablesMatchingSamePattern() throws Exception {
        Connection jdbcConnection = DriverManagerConnectionsFactory.getIT().fetchConnection("org.hsqldb.jdbcDriver",
                "jdbc:hsqldb:mem:" + "tempdb", "sa", "");
        DdlExecutor.executeDdlFile(TestUtils.getFile("sql/hypersonic_dataset_pattern_test.sql"), jdbcConnection);
        IDatabaseConnection connection = new DatabaseConnection(jdbcConnection, new DatabaseConfig());

        try {
            String tableName = "PATTERN_LIKE_TABLE_X_";
            String[] columnNames = { "VARCHAR_COL_XUNDERSCORE" };

            String sql = "select * from " + tableName;
            ForwardOnlyResultSetTable resultSetTable = new ForwardOnlyResultSetTable(tableName, sql, connection);
            ResultSetTableMetaData metaData = (ResultSetTableMetaData) resultSetTable.getTableMetaData();

            Column[] columns = metaData.getColumns();

            assertEquals("column count", columnNames.length, columns.length);

            for (String columnName : columnNames) {
                Column column = Columns.getColumn(columnName, columns);
                assertEquals(columnName, columnName, column.getColumnName());
            }
        } finally {
            DdlExecutor.executeSql(jdbcConnection, "DROP SCHEMA PUBLIC IF EXISTS CASCADE");
            DdlExecutor.executeSql(jdbcConnection, "DROP SCHEMA TEST_SCHEMA IF EXISTS CASCADE");
            DdlExecutor.executeSql(jdbcConnection, "SET SCHEMA PUBLIC");
            jdbcConnection.close();
        }
    }

}
