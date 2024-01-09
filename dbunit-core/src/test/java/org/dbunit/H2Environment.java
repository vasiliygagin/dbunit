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

import org.dbunit.operation.DatabaseOperation;

import io.github.vasiliygagin.dbunit.jdbc.DatabaseConfig;

public class H2Environment extends DatabaseEnvironment {

    private static final String databaseName = "target/h2/test";

    public H2Environment() throws Exception {
        super(databaseName, new H2DatabaseProfile(), new DatabaseConfig());
    }

    @Override
    protected String buildConnectionUrl(String databaseName) {
        return "jdbc:h2:" + databaseName;
    }

    @Override
    public void closeConnection() throws Exception {
        DatabaseOperation.DELETE_ALL.execute(getOpenedDatabase().getConnection(), getInitDataSet());
    }

    private static class H2DatabaseProfile extends DatabaseProfile {

        public H2DatabaseProfile() {
            super("org.h2.Driver", "jdbc:h2:" + databaseName, "PUBLIC", "sa", "", "hypersonic.sql", true,
                    new String[] { "BLOB", "CLOB", "SCROLLABLE_RESULTSET", "INSERT_IDENTITY", "TRUNCATE_TABLE",
                            "SDO_GEOMETRY", "XML_TYPE" });
        }
    }
}
