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

package org.dbunit.database;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.SQLException;

import org.dbunit.AbstractDatabaseIT;
import org.dbunit.DefaultDatabaseTester;
import org.dbunit.DefaultOperationListener;
import org.dbunit.IDatabaseTester;
import org.dbunit.database.metadata.MetadataManager;
import org.junit.Before;
import org.junit.Test;

/**
 * @author Manuel Laflamme
 * @version $Revision$
 * @since Mar 26, 2002
 */
public abstract class AbstractDatabaseConnectionIT extends AbstractDatabaseIT {

    private String schema;

    public AbstractDatabaseConnectionIT() throws Exception {
    }

    @Before
    public final void setUp1() throws Exception {
        this.schema = environment.getSchema();
    }

    @Test
    public final void testGetRowCount() throws Exception {
        DatabaseConnection customizedConnection = database.getConnection();
        assertEquals("EMPTY_TABLE", 0, customizedConnection.getRowCount("EMPTY_TABLE", null));
        assertEquals("EMPTY_TABLE", 0, customizedConnection.getRowCount("EMPTY_TABLE"));

        assertEquals("TEST_TABLE", 6, customizedConnection.getRowCount("TEST_TABLE", null));
        assertEquals("TEST_TABLE", 6, customizedConnection.getRowCount("TEST_TABLE"));

        assertEquals("PK_TABLE", 1, customizedConnection.getRowCount("PK_TABLE", "where PK0 = 0"));
    }

    @Test
    public final void testGetRowCount_NonexistingSchema() throws Exception {
        String nonexistingSchema = environment.getSchema() + "_444_XYZ_TEST";
        this.schema = nonexistingSchema;

        IDatabaseTester dbTester = this.newDatabaseTester(nonexistingSchema);
        try {
            IDatabaseConnection dbConnection = dbTester.getConnection();

            assertEquals(convertString(nonexistingSchema), dbConnection.getSchema());
            try {
                dbConnection.getRowCount("TEST_TABLE");
                fail("Should not be able to retrieve row count for non-existing schema " + nonexistingSchema);
            } catch (SQLException expected) {
                // All right
            }
        } finally {
            // Reset the testers schema for subsequent tests (environment.dbTester is a
            // singleton)
            dbTester.setSchema(environment.getSchema());
        }
    }

    @Test
    public final void testGetRowCount_NoSchemaSpecified() throws Exception {
        DatabaseConnection customizedConnection = database.getConnection();
        this.schema = null;
        IDatabaseTester dbTester = this.newDatabaseTester(this.schema);
        try {
            IDatabaseConnection dbConnection = dbTester.getConnection();

            assertEquals(null, dbConnection.getSchema());
            assertEquals("TEST_TABLE", 6, customizedConnection.getRowCount("TEST_TABLE", null));
        } finally {
            // Reset the testers schema for subsequent tests (environment.dbTester is a
            // singleton)
            dbTester.setSchema(environment.getSchema());
        }
    }

    private DefaultDatabaseTester newDatabaseTester(String schema) throws Exception {
        DatabaseConnection customizedConnection = database.getConnection();
        Connection jdbcConnection = database.getJdbcConnection();

        io.github.vasiliygagin.dbunit.jdbc.DatabaseConfig databaseConfig = environment.getDatabaseConfig();
        MetadataManager metadataManager = new MetadataManager(jdbcConnection, databaseConfig, null,
                environment.getSchema());
        customizedConnection = new DatabaseConnection(jdbcConnection, databaseConfig, environment.getSchema(),
                metadataManager);

        final DatabaseConnection connection = new DatabaseConnection(jdbcConnection, databaseConfig, this.schema,
                metadataManager);
        DefaultDatabaseTester tester = new DefaultDatabaseTester(connection);
        tester.setOperationListener(new DefaultOperationListener() {

            @Override
            public void operationSetUpFinished(IDatabaseConnection connection) {
            }

            @Override
            public void operationTearDownFinished(IDatabaseConnection connection) {
            }
        });
        tester.setSchema(schema);
        return tester;
    }
}
