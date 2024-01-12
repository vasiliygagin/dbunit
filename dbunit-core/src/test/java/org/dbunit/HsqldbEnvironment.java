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

import org.dbunit.ext.hsqldb.HsqldbDatabaseConfig;
import org.dbunit.operation.DatabaseOperation;

/**
 * @author Manuel Laflamme
 * @version $Revision$
 * @since Feb 18, 2002
 */
public class HsqldbEnvironment extends DatabaseTestingEnvironment {

    private static final String databaseName = ".";

    public HsqldbEnvironment() throws Exception {
        super(databaseName, new HypersonicDatabaseProfile(), new HsqldbDatabaseConfig());
    }

    @Override
    public void closeConnection() throws Exception {
        DatabaseOperation.DELETE_ALL.execute(getOpenedDatabase().getConnection(), getInitDataSet());
    }

    @Override
    protected String buildConnectionUrl(String databaseName) {
        return "jdbc:hsqldb:mem:" + databaseName;
    }

    private static class HypersonicDatabaseProfile extends DatabaseTestingProfile {

        public HypersonicDatabaseProfile() {
            super("org.hsqldb.jdbcDriver", "jdbc:hsqldb:mem:" + databaseName, "PUBLIC", "sa", "", "hypersonic.sql",
                    false, new String[] { "BLOB", "CLOB", "SCROLLABLE_RESULTSET", "INSERT_IDENTITY", "TRUNCATE_TABLE",
                            "SDO_GEOMETRY", "XML_TYPE" });
        }
    }

    @Override
    public void closeDatabase(Database database) {
        try {
            DdlExecutor.executeSql(database.getJdbcConnection(), "SHUTDOWN IMMEDIATELY");
        } catch (SQLException exc) {
            // TODO Auto-generated catch block
            exc.printStackTrace();
        }
    }
}
