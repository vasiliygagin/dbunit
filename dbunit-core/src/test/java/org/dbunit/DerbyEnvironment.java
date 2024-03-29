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

package org.dbunit;

import java.sql.SQLException;

import io.github.vasiliygagin.dbunit.jdbc.DatabaseConfig;

public class DerbyEnvironment extends DatabaseTestingEnvironment {

    private static final String databaseName = ".";
    private static final String url = "jdbc:derby:memory:" + databaseName;

    public DerbyEnvironment() throws Exception {
        super(databaseName, prepare(new DerbyDatabaseProfile()), new DatabaseConfig());
    }

    private static DatabaseTestingProfile prepare(DatabaseTestingProfile profile) {
        // FileHelper.deleteDirectory(new File("./target/derby_db"));
        return profile;
    }

    @Override
    protected String buildConnectionUrl(String databaseName) {
        return url + ";create=true";
    }

    private static class DerbyDatabaseProfile extends DatabaseTestingProfile {

        /**
         *
         */

        public DerbyDatabaseProfile() {
            super("org.apache.derby.jdbc.EmbeddedDriver", url + ";create=true", "APP", "APP", "APP", "derby.sql", false,
                    new String[] { "VARBINARY", "BLOB", "CLOB", "TRANSACTION", "SCROLLABLE_RESULTSET",
                            "INSERT_IDENTITY", "TRUNCATE_TABLE", "SDO_GEOMETRY", "XML_TYPE" });
        }
    }

    @Override
    public void closeDatabase(Database database) {
        try {
            openConnection(url + ";shutdown=true;");
        } catch (SQLException exc) {
            // expecting ERROR 08006
        }
    }
}
