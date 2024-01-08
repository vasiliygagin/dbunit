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

/**
 * @author John Hurst (adpated from Manuel Laflamme: OracleEnvironment)
 * @version $Revision$
 * @since DbUnit 2.4.7
 */
public class Oracle10Environment extends OracleEnvironment {

    public Oracle10Environment() throws Exception {
        super(new Oracle10DatabaseProfile(), new Oracle10DatabaseConfig());
    }

    /**
     *
     */
    private static class Oracle10DatabaseProfile extends DatabaseProfile {

        public Oracle10DatabaseProfile() {
            super("oracle.jdbc.OracleDriver", "jdbc:oracle:thin:@localhost:1521:XE", "DBUNIT", "dbunit", "dbunit",
                    "oracle.sql", false, new String[] { "INSERT_IDENTITY", "SCROLLABLE_RESULTSET" });
        }
    }
}
