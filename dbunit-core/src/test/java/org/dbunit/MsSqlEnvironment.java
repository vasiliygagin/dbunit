/*
 *
 * The DbUnit Database Testing Framework
 * Copyright (C)2002-2017, DbUnit.org
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

public class MsSqlEnvironment extends DatabaseEnvironment {

    private static final String databaseName = "//localhost:1433";

    public MsSqlEnvironment() throws Exception {
        super(databaseName, new MsSqlDatabaseProfile(), new MsSqlDatabaseConfig());
    }

    @Override
    protected String buildConnectionUrl(String databaseName) {
        return "jdbc:sqlserver:" + databaseName
                + ";user=sa;password=theSaPassword1234;Trusted_Connection=True;SelectMethod=cursor";
    }

    /**
     * Preserve case for MS SQL
     *
     * @see DatabaseEnvironment#convertString(String)
     */
    @Override
    public String convertString(final String str) {
        return str;
    }

    private static class MsSqlDatabaseProfile extends DatabaseProfile {

        public MsSqlDatabaseProfile() {
            super("com.microsoft.sqlserver.jdbc.SQLServerDriver",
                    "jdbc:sqlserver:" + databaseName
                            + ";user=sa;password=theSaPassword1234;Trusted_Connection=True;SelectMethod=cursor",
                    "dbo", "sa", "theSaPassword1234", "mssql.sql", false,
                    new String[] { "BLOB", "CLOB", "SCROLLABLE_RESULTSET", "SDO_GEOMETRY", "XML_TYPE" });
        }
    }
}