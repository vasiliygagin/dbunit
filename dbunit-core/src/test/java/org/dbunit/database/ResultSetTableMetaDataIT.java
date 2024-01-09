package org.dbunit.database;

import static org.junit.Assert.assertEquals;

import org.dbunit.AbstractDatabaseIT;
import org.dbunit.DdlExecutor;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.Columns;
import org.dbunit.dataset.IDataSet;
import org.dbunit.testutil.TestUtils;
import org.junit.Test;

/**
 * @author gommma (gommma AT users.sourceforge.net)
 * @author Last changed by: $Author$
 * @version $Revision$ $Date$
 * @since 2.4.0
 */
public class ResultSetTableMetaDataIT extends AbstractDatabaseIT {

    public ResultSetTableMetaDataIT() throws Exception {
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
    @Test
    public void testGetColumnsForTablesMatchingSamePattern() throws Exception {
        DdlExecutor.executeDdlFile(environment, database.getJdbcConnection(),
                TestUtils.getFile("sql/hypersonic_dataset_pattern_test.sql"));

        String tableName = "PATTERN_LIKE_TABLE_X_";
        String[] columnNames = { "VARCHAR_COL_XUNDERSCORE" };

        String sql = "select * from " + tableName;
        ForwardOnlyResultSetTable resultSetTable = new ForwardOnlyResultSetTable(tableName, sql,
                database.getConnection());
        ResultSetTableMetaData metaData = (ResultSetTableMetaData) resultSetTable.getTableMetaData();

        Column[] columns = metaData.getColumns();

        assertEquals("column count", columnNames.length, columns.length);

        for (String columnName : columnNames) {
            Column column = Columns.getColumn(columnName, columns);
            assertEquals(columnName, columnName, column.getColumnName());
        }
    }

}
