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

/**
 * @author Manuel Laflamme
 * @version $Revision$
 * @since Feb 18, 2002
 */
public class HypersonicEnvironment extends DatabaseEnvironment {

    public HypersonicEnvironment() throws Exception {
        super(new HypersonicDatabaseProfile(), new DatabaseConfig());
    }

    @Override
    public void closeConnection() throws Exception {
        DatabaseOperation.DELETE_ALL.execute(getConnection(), getInitDataSet());
    }

    private static class HypersonicDatabaseProfile extends DatabaseProfile {

        public HypersonicDatabaseProfile() {
            super("org.hsqldb.jdbcDriver", "jdbc:hsqldb:mem:.", "PUBLIC", "sa", "", "hypersonic.sql", false,
                    new String[] { "BLOB", "CLOB", "SCROLLABLE_RESULTSET", "INSERT_IDENTITY", "TRUNCATE_TABLE",
                            "SDO_GEOMETRY", "XML_TYPE" });
        }
    }
}
