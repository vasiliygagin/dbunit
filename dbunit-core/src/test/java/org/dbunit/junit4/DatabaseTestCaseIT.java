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

import static org.junit.Assert.assertEquals;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import org.dbunit.assertion.TestDataSet;
import org.dbunit.assertion.TestTable;
import org.dbunit.dataset.IDataSet;
import org.dbunit.junit.DriverManagerConnection;
import org.dbunit.junit.SqlAfter;
import org.dbunit.junit.SqlBefore;
import org.junit.Test;

/**
 * Proper integration test, need to test that:
 * 1. existing data is deleted before test
 * 2. new data is inserted before test
 * 3. data is restored after test
 * @author Vasiliy Gagin
 */
@DriverManagerConnection(driver = "org.hsqldb.jdbcDriver", url = "jdbc:hsqldb:mem:.", user = "sa", password = "")
public class DatabaseTestCaseIT extends DatabaseTestCase {

    @Override
    protected IDataSet getDataSet() throws Exception {
        TestTable table = new TestTable("TEST_TABLE", "ID", "NAME");
        table.addRow(1, "NAME1");
        table.addRow(2, "NAME2");
        return new TestDataSet(table);
    }

    @Test
    @SqlBefore(filePath = "src/test/resources/sql/test_table.sql")
    @SqlAfter(filePath = "src/test/resources/sql/test_table_drop.sql")
    public void initializesWithConfiguredDataSet() throws Exception {
        Connection jdbcConnection = dbUnit.getJdbcConnection();
        try (Statement statement = jdbcConnection.createStatement();) {
            ResultSet rs = statement.executeQuery("select ID, NAME from TEST_TABLE order by ID");
            rs.next();
            assertEquals(1, rs.getInt("ID"));
            assertEquals("NAME1", rs.getString("NAME"));
            rs.next();
            assertEquals(2, rs.getInt("ID"));
            assertEquals("NAME2", rs.getString("NAME"));
            assertEquals(false, rs.next());
        }
    }
}
