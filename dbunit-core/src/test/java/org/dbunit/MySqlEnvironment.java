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
public class MySqlEnvironment extends DatabaseEnvironment {

    private static final String databaseName = "//localhost:3306/dbunit";

    public MySqlEnvironment() throws Exception {
        super(databaseName, new MySqlDatabaseProfile(), new MySqlDatabaseConfig());
    }

    @Override
    protected String buildConnectionUrl(String databaseName) {
        return "jdbc:mysql:" + databaseName;
    }

    /**
     * Preserve case for MySQL
     *
     * @see DatabaseEnvironment#convertString(String)
     */
    @Override
    public String convertString(String str) {
        return str;
    }

    private static class MySqlDatabaseProfile extends DatabaseProfile {

        public MySqlDatabaseProfile() {
            super("com.mysql.jdbc.Driver", "jdbc:mysql:" + databaseName, "", "dbunit", "dbunit", "mysql.sql", false,
                    new String[] { "BLOB", "CLOB,SCROLLABLE_RESULTSET", "INSERT_IDENTITY", "SDO_GEOMETRY",
                            "XML_TYPEBLOB", "CLOB", "SCROLLABLE_RESULTSET", "INSERT_IDENTITY", "SDO_GEOMETRY",
                            "XML_TYPE" });
        }
    }
}