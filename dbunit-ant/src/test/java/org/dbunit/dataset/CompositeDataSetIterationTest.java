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

package org.dbunit.dataset;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

import org.dbunit.database.DatabaseConfig;
import org.dbunit.database.DatabaseConnection;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.database.QueryDataSet;
import org.dbunit.dataset.datatype.DataType;
import org.dbunit.dataset.xml.FlatXmlWriter;
import org.dbunit.ext.hsqldb.HsqldbDataTypeFactory;

import junit.framework.TestCase;

/**
 * Test Case for issue #1721870
 *
 * @author Sebastien Le Callonnec
 * @version $Revision$
 * @since Mar 11, 2008
 */
public class CompositeDataSetIterationTest extends TestCase {

    private Connection jdbcConnection;
    private IDatabaseConnection connection;

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        Class.forName("org.hsqldb.jdbcDriver");
        Connection connection1 = DriverManager.getConnection("jdbc:hsqldb:" + "mem:tempdb", "sa", "");
        this.jdbcConnection = connection1;
        final File ddlFile = new File("src/test/resources/sql/hypersonic_simple_dataset.sql");
        final Connection connection2 = jdbcConnection;

        final String sql = readSqlFromFile(ddlFile);

        executeSql(connection2, sql);

        this.connection = new DatabaseConnection(jdbcConnection);
        DatabaseConfig config = connection.getConfig();
        config.setProperty(DatabaseConfig.PROPERTY_DATATYPE_FACTORY, new HsqldbDataTypeFactory());
    }

    private String readSqlFromFile(final File ddlFile) throws IOException {
        final BufferedReader sqlReader = new BufferedReader(new FileReader(ddlFile));
        final StringBuilder sqlBuffer = new StringBuilder();
        while (sqlReader.ready()) {
            String line = sqlReader.readLine();
            if (!line.startsWith("-")) {
                sqlBuffer.append(line);
            }
        }

        sqlReader.close();

        return sqlBuffer.toString();
    }

    private void executeSql(final Connection connection2, final String sql) throws SQLException {

        try (final Statement statement = connection2.createStatement();) {
            statement.execute(sql);
        }
    }

    @Override
    protected void tearDown() throws Exception {
        super.tearDown();
        final Connection connection = this.jdbcConnection;

        executeSql(connection, "SHUTDOWN IMMEDIATELY");
        this.jdbcConnection.close();
    }

    public void testMe() throws Exception {

        // 1. QueryDataSet
        QueryDataSet queryDataSet = new QueryDataSet(connection);
        queryDataSet.addTable("B", "select * from B");
        queryDataSet.addTable("C", "select * from C");

        // 2. Hard-coded data set
        DefaultDataSet plainDataSet = new DefaultDataSet();

        Column id = new Column("id", DataType.DOUBLE);
        Column name = new Column("name", DataType.VARCHAR);

        Column[] cols = { id, name };

        DefaultTable aTable = new DefaultTable("D", cols);
        Object[] row1 = { new Long(1), "D1" };
        Object[] row2 = { new Long(2), "D2" };

        aTable.addRow(row1);
        aTable.addRow(row2);

        plainDataSet.addTable(aTable);

        // 3. Composite
        CompositeDataSet compositeDataSet = new CompositeDataSet(queryDataSet, plainDataSet);

        // 4. Write
        try {
            FlatXmlWriter datasetWriter = new FlatXmlWriter(new FileOutputStream("target/full.xml"));
            datasetWriter.setIncludeEmptyTable(true);
            datasetWriter.write(compositeDataSet);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }
}
