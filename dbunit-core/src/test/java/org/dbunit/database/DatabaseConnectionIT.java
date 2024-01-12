/*
 * DatabaseConnectionTest.java   Mar 26, 2002
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

package org.dbunit.database;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.util.Locale;

import org.dbunit.DatabaseUnitException;
import org.dbunit.database.metadata.MetadataManager;
import org.dbunit.dataset.ITable;
import org.junit.Test;

/**
 * @author Manuel Laflamme
 * @version $Revision$
 * @since Mar 26, 2002
 */
public class DatabaseConnectionIT extends AbstractDatabaseConnectionIT {

    public DatabaseConnectionIT() throws Exception {
    }

    @Override
    protected String convertString(String str) throws Exception {
        return environment.convertString(str);
    }

    @Test
    public void testCreateNullConnection() throws Exception {
        IDatabaseConnection validConnection = getConnection32();
        try {
            Connection jdbcConnection = validConnection.getConnection();
            DatabaseConfig config = new DatabaseConfig();
            MetadataManager metadataManager = new MetadataManager(jdbcConnection, config, null, null);
            new DatabaseConnection(null, config, metadataManager);
            fail("Should not be able to create a database connection without a JDBC connection");
        } catch (IllegalArgumentException expected) {
            // all right
        }
    }

    @Test
    public void testCreateConnectionWithNonExistingSchemaAndStrictValidation() throws Exception {
        String schema = environment.convertString("XYZ_INVALID_SCHEMA_1642344539");
        IDatabaseConnection validConnection = getConnection32();
        // Try to create a database connection with an invalid schema
        try {
            boolean validate = true;
            DatabaseConfig config = new DatabaseConfig();
            Connection jdbcConnection = validConnection.getConnection();
            MetadataManager metadataManager = new MetadataManager(jdbcConnection, config, null, schema);
            new DatabaseConnection(jdbcConnection, config, schema, validate, metadataManager);
            fail("Should not be able to create a database connection object with an unknown schema.");
        } catch (DatabaseUnitException expected) {
            String expectedMsg = "The given schema '" + convertString(schema) + "' does not exist.";
            assertEquals(expectedMsg, expected.getMessage());
        }
    }

    @Test
    public void testCreateConnectionWithNonExistingSchemaAndLenientValidation() throws Exception {
        String schema = environment.convertString("XYZ_INVALID_SCHEMA_1642344539");
        IDatabaseConnection validConnection = getConnection32();
        // Try to create a database connection with an invalid schema
        boolean validate = false;
        Connection jdbcConnection = validConnection.getConnection();
        DatabaseConfig config = new DatabaseConfig();
        MetadataManager metadataManager = new MetadataManager(jdbcConnection, config, null, schema);
        DatabaseConnection dbConnection = new DatabaseConnection(jdbcConnection, config, schema, validate,
                metadataManager);
        assertNotNull(dbConnection);
    }

    @Test
    public void testCreateConnectionWithSchemaDbStoresUpperCaseIdentifiers() throws Exception {
        IDatabaseConnection validConnection = getConnection32();
        String schema = validConnection.getSchema();
        assertNotNull("Precondition: schema of connection must not be null", schema);

        DatabaseMetaData metaData = validConnection.getConnection().getMetaData();
        if (metaData.storesUpperCaseIdentifiers()) {
            boolean validate = true;
            Connection jdbcConnection = validConnection.getConnection();
            DatabaseConfig config = new DatabaseConfig();
            MetadataManager metadataManager = new MetadataManager(jdbcConnection, config, null,
                    schema.toLowerCase(Locale.ENGLISH));
            DatabaseConnection dbConnection = new DatabaseConnection(jdbcConnection, config,
                    schema.toLowerCase(Locale.ENGLISH), validate, metadataManager);
            assertNotNull(dbConnection);
            assertEquals(schema.toUpperCase(Locale.ENGLISH), dbConnection.getSchema());
        } else {
            // skip this test
            assertTrue(true);
        }
    }

    @Test
    public void testCreateConnectionWithSchemaDbStoresLowerCaseIdentifiers() throws Exception {
        IDatabaseConnection validConnection = getConnection32();
        String schema = validConnection.getSchema();
        assertNotNull("Precondition: schema of connection must not be null", schema);

        DatabaseMetaData metaData = validConnection.getConnection().getMetaData();
        if (metaData.storesLowerCaseIdentifiers()) {
            boolean validate = true;
            Connection jdbcConnection = validConnection.getConnection();
            DatabaseConfig config = new DatabaseConfig();
            MetadataManager metadataManager = new MetadataManager(jdbcConnection, config, null,
                    schema.toUpperCase(Locale.ENGLISH));
            DatabaseConnection dbConnection = new DatabaseConnection(jdbcConnection, config,
                    schema.toUpperCase(Locale.ENGLISH), validate, metadataManager);
            assertNotNull(dbConnection);
            assertEquals(schema.toLowerCase(Locale.ENGLISH), dbConnection.getSchema());
        } else {
            // skip this test
            assertTrue(true);
        }
    }

    @Test
    public void testCreateQueryWithPreparedStatement() throws Exception {
        IDatabaseConnection connection = getConnection32();
        PreparedStatement pstmt = connection.getConnection()
                .prepareStatement("select * from TEST_TABLE where COLUMN0=?");

        try {
            pstmt.setString(1, "row 1 col 0");
            ITable table = connection.createTable("MY_TABLE", pstmt);
            assertEquals(1, table.getRowCount());
            assertEquals(4, table.getTableMetaData().getColumns().length);
            assertEquals("row 1 col 1", table.getValue(0, "COLUMN1"));

            // Now reuse the prepared statement
            pstmt.setString(1, "row 2 col 0");
            ITable table2 = connection.createTable("MY_TABLE", pstmt);
            assertEquals(1, table2.getRowCount());
            assertEquals(4, table2.getTableMetaData().getColumns().length);
            assertEquals("row 2 col 1", table2.getValue(0, "COLUMN1"));
        } finally {
            pstmt.close();
        }
    }

    private final IDatabaseConnection getConnection32() throws Exception {
        return database.getConnection();
    }
}
