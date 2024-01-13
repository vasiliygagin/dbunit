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
package org.dbunit.junit4;

import static org.dbunit.junit.internal.DbunitTestCaseTestRunner.assertAfter;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import org.dbunit.database.IDatabaseConnection;
import org.dbunit.dataset.Column;
import org.dbunit.dataset.DefaultDataSet;
import org.dbunit.dataset.DefaultTable;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.datatype.DataType;
import org.dbunit.dataset.xml.XmlDataSet;
import org.dbunit.junit.DriverManagerConnection;
import org.dbunit.operation.DatabaseOperation;
import org.dbunit.testutil.TestUtils;
import org.junit.Test;

/**
 * @author Vasiliy Gagin
 */
@DriverManagerConnection(driver = "org.hsqldb.jdbcDriver", url = "jdbc:hsqldb:mem:.", user = "sa", password = "")
public class DatabaseTestCase_DataSet_IT extends DatabaseInternalTestCase {

    @Override
    protected IDataSet getDataSet() throws Exception {
        return new XmlDataSet(TestUtils.getFileReader("xml/dataSetTest.xml"));
    }

    @Test
    public void testCustomDataSet() throws Exception {

        Connection jdbcConnection = getJdbcConnection();

        try ( //
                Statement statement = jdbcConnection.createStatement();
                ResultSet rs = statement.executeQuery("select count(1) from PARENT"); //
        ) {
            assertTrue(rs.next());
            assertEquals(1, rs.getInt(1));
        }

        assertAfter(() -> {
            try ( //
                    Statement statement = jdbcConnection.createStatement();
                    ResultSet rs = statement.executeQuery("select count(1) from PARENT"); //
            ) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1));
            }
        });
    }

    @Test
    public void testOperation() throws Exception {

        IDatabaseConnection connection = getConnection();
        DefaultTable table = new DefaultTable("PARENT",
                new Column[] { new Column("ID", DataType.INTEGER), new Column("DESC", DataType.VARCHAR) });
        table.addRow(new Object[] { 2, "PARENT 2" });
        IDataSet dataSet = new DefaultDataSet(table);

        DatabaseOperation.INSERT.execute(connection, dataSet); // inserting 2nd time

        Connection jdbcConnection = getJdbcConnection();
        try ( //
                Statement statement = jdbcConnection.createStatement();
                ResultSet rs = statement.executeQuery("select count(1) from PARENT"); //
        ) {
            assertTrue(rs.next());
            assertEquals(2, rs.getInt(1));
        }

        assertAfter(() -> {
            try ( //
                    Statement statement = jdbcConnection.createStatement();
                    ResultSet rs = statement.executeQuery("select count(1) from PARENT"); //
            ) {
                assertTrue(rs.next());
                assertEquals(0, rs.getInt(1));
            }
        });
    }
}
