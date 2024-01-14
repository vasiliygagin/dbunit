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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DatabaseEnvironmentLoader {

    private static final Logger logger = LoggerFactory.getLogger(DatabaseEnvironmentLoader.class);

    private static DatabaseTestingEnvironment INSTANCE = null;

    public static DatabaseTestingEnvironment getInstance() throws Exception {
        if (INSTANCE == null) {
            String profileName = System.getProperty("test.environment");
            if (profileName == null) {
                profileName = "hsqldb";
            }

            if (profileName.equals("hsqldb")) {
                INSTANCE = new HsqldbEnvironment();
            } else if (profileName.equals("oracle")) {
                INSTANCE = new OracleEnvironment();
            } else if (profileName.equals("oracle10")) {
                INSTANCE = new Oracle10Environment();
            } else if (profileName.equals("postgresql")) {
                INSTANCE = new PostgresqlEnvironment();
            } else if (profileName.equals("mysql")) {
                INSTANCE = new MySqlEnvironment();
            } else if (profileName.equals("derby")) {
                INSTANCE = new DerbyEnvironment();
            } else if (profileName.equals("h2")) {
                INSTANCE = new H2Environment();
            } else if (profileName.equals("mssql")) {
                INSTANCE = new MsSqlEnvironment();
            } else {
                logger.warn("getInstance: activeProfile={} not known, using generic profile", profileName);
                INSTANCE = new GenericEnvironment(profileName);
            }
        }

        return INSTANCE;
    }
}
