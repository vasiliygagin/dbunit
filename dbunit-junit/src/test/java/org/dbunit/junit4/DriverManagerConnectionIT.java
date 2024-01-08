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

import static org.dbunit.junit4.DbunitTestCaseTestRunner.assertAfter;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Statement;

import org.dbunit.junit.DbUnitFacade;
import org.dbunit.junit.DriverManagerConnection;
import org.dbunit.junit.internal.TestContext;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * @author Vasiliy Gagin
 */
@RunWith(DbunitTestCaseTestRunner.class)
@DriverManagerConnection(driver = "org.hsqldb.jdbcDriver", url = "jdbc:hsqldb:mem:.", user = "sa", password = "")
public class DriverManagerConnectionIT {

    @Rule
    public final DbUnitFacade dbUnit = new DbUnitFacade();

    @Test
    public void testConnectionLifecycle() throws Exception {
        TestContext testContext = dbUnit.getTestContext();

        DatabaseMetaData metadata = dbUnit.getJdbcConnection().getMetaData();
        assertNotNull(testContext.getConnection());

        assertEquals("HSQL Database Engine", metadata.getDatabaseProductName());
    }

    @Test
    public void testDefaultDataSet() throws Exception {
        Connection jdbcConnection = dbUnit.getJdbcConnection();

        try ( //
                Statement statement = jdbcConnection.createStatement(); //
        ) {

            ResultSet rs = statement.executeQuery("select count(1) from PARENT");
            rs.next();
            assertEquals(0, rs.getInt(1));

            statement.executeUpdate("insert into PARENT values (1, 'PARENT 1')");

            rs = statement.executeQuery("select count(1) from PARENT");
            rs.next();
            assertEquals(1, rs.getInt(1));
        }

        assertAfter(() -> {
            try ( //
                    Statement statement = jdbcConnection.createStatement();
                    ResultSet rs = statement.executeQuery("select count(1) from PARENT"); //
            ) {
                rs.next();
                assertEquals(0, rs.getInt(1));
            }
        });
    }
}
