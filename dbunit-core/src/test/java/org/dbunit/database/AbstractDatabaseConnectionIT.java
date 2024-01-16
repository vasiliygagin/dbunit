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
import org.dbunit.database.metadata.MetadataManager;
import org.junit.Before;
import org.junit.Test;

import io.github.vasiliygagin.dbunit.jdbc.DatabaseConfig;

/**
 * @author Manuel Laflamme
 * @version $Revision$
 * @since Mar 26, 2002
 */
public abstract class AbstractDatabaseConnectionIT extends AbstractDatabaseIT {

    public AbstractDatabaseConnectionIT() throws Exception {
    }

    @Before
    public final void setUp1() throws Exception {
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

        IDatabaseConnection dbConnection = getConnection3(nonexistingSchema);

        assertEquals(convertString(nonexistingSchema), dbConnection.getSchema());
        try {
            dbConnection.getRowCount("TEST_TABLE");
            fail("Should not be able to retrieve row count for non-existing schema " + nonexistingSchema);
        } catch (SQLException expected) {
            // All right
        }
    }

    @Test
    public final void testGetRowCount_NoSchemaSpecified() throws Exception {
        DatabaseConnection customizedConnection = database.getConnection();
        IDatabaseConnection dbConnection = getConnection3(null);

        assertEquals(null, dbConnection.getSchema());
        assertEquals("TEST_TABLE", 6, customizedConnection.getRowCount("TEST_TABLE", null));
    }

    private IDatabaseConnection getConnection3(String schema) throws Exception {
        Connection jdbcConnection = this.database.getJdbcConnection();

        DatabaseConfig databaseConfig = this.environment.getDatabaseConfig();
        MetadataManager metadataManager = new MetadataManager(jdbcConnection, databaseConfig, null,
                this.environment.getSchema());

        return new DatabaseConnection(jdbcConnection, databaseConfig, schema, metadataManager);
    }
}
