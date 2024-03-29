/*
 *
 * The DbUnit Database Testing Framework
 * Copyright (C)2002-2009, DbUnit.org
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

/**
 * @author John Hurst (adapted from Manuel Laflamme: OracleEnvironment)
 * @version $Revision$
 * @since DbUnit 2.4.7
 */
public class PostgresqlEnvironment extends DatabaseTestingEnvironment {

    private static final String databaseName = "//localhost/dbunit";

    public PostgresqlEnvironment() throws Exception {
        super(databaseName, new PostgresqlDatabaseProfile(), new PostgresqlDatabaseConfig());
    }

    @Override
    protected String buildConnectionUrl(String databaseName) {
        return "jdbc:postgresql:" + databaseName;
    }

    @Override
    public String convertString(String str) {
        return str == null ? null : str.toLowerCase();
    }

    private static class PostgresqlDatabaseProfile extends DatabaseTestingProfile {

        public PostgresqlDatabaseProfile() {
            super("org.postgresql.Driver", "jdbc:postgresql:" + databaseName, "public", "dbunit", "dbunit",
                    "postgresql.sql", false, new String[] { "INSERT_IDENTITY", "CLOB", "BLOB", "SCROLLABLE_RESULTSET",
                            "SDO_GEOMETRY", "XML_TYPE" });
        }
    }
}
