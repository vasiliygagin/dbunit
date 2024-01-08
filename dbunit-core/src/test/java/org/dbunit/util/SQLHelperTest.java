/*
 *
 * The DbUnit Database Testing Framework
 * Copyright (C)2005, DbUnit.org
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

package org.dbunit.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import org.dbunit.AbstractHSQLTestCase;
import org.junit.Test;

import com.mockobjects.sql.MockDatabaseMetaData;

/**
 * @author Felipe Leme (dbunit@felipeal.net)
 * @version $Revision$
 * @since Nov 5, 2005
 */
public class SQLHelperTest extends AbstractHSQLTestCase {

    public SQLHelperTest() {
        super("hypersonic_dataset.sql");
    }

    @Test
    public void testGetPrimaryKeyColumn() throws SQLException {
        String[] tables = { "A", "B", "C", "D", "E", "F", "G", "H" };
        Connection conn = getConnection().getConnection();
        assertNotNull("didn't get a connection", conn);
        for (String table : tables) {
            String expectedPK = "PK" + table;
            String actualPK = SQLHelper.getPrimaryKeyColumn(conn, table);
            assertNotNull(actualPK);
            assertEquals("primary key column for table " + table + " does not match", expectedPK, actualPK);
        }
    }

    @Test
    public void testGetDatabaseInfoWithException() throws Exception {
        final String productName = "Some product";
        final String exceptionText = "Dummy exception to simulate unimplemented operation exception as occurs "
                + "in sybase 'getDatabaseMajorVersion()' (com.sybase.jdbc3.utils.UnimplementedOperationException)";

        DatabaseMetaData metaData = new MockDatabaseMetaData() {

            @Override
            public String getDatabaseProductName() throws SQLException {
                return productName;
            }

            @Override
            public String getDatabaseProductVersion() throws SQLException {
                return null;
            }

            @Override
            public int getDriverMajorVersion() {
                return -1;
            }

            @Override
            public int getDriverMinorVersion() {
                return -1;
            }

            @Override
            public String getDriverName() throws SQLException {
                return null;
            }

            @Override
            public String getDriverVersion() throws SQLException {
                return null;
            }

            @Override
            public int getDatabaseMajorVersion() throws SQLException {
                throw new SQLException(exceptionText);
            }

            @Override
            public int getDatabaseMinorVersion() throws SQLException {
                return -1;
            }
        };
        String info = SQLHelper.getDatabaseInfo(metaData);
        assertNotNull(info);
        assertTrue(info.indexOf(productName) > -1);
        assertTrue(info.indexOf(SQLHelper.ExceptionWrapper.NOT_AVAILABLE_TEXT) > -1);
    }
}
