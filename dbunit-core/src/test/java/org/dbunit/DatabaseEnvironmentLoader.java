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

import io.github.vasiliygagin.dbunit.jdbc.DatabaseConfig;

public class DatabaseEnvironmentLoader {

    private static final Logger logger = LoggerFactory.getLogger(DatabaseEnvironmentLoader.class);

    private static DatabaseEnvironment INSTANCE = null;

    public static DatabaseEnvironment getInstance(String profileName) throws Exception {
        if (INSTANCE == null) {
            final DatabaseProfile profile = new DatabaseProfile(profileName);

            if (profile.profileName.equals("hsqldb")) {
                INSTANCE = new HypersonicEnvironment(profile);
            } else if (profile.profileName.equals("oracle")) {
                INSTANCE = new OracleEnvironment(profile);
            } else if (profile.profileName.equals("oracle10")) {
                INSTANCE = new Oracle10Environment(profile);
            } else if (profile.profileName.equals("postgresql")) {
                INSTANCE = new PostgresqlEnvironment(profile);
            } else if (profile.profileName.equals("mysql")) {
                INSTANCE = new MySqlEnvironment(profile);
            } else if (profile.profileName.equals("derby")) {
                INSTANCE = new DerbyEnvironment(profile);
            } else if (profile.profileName.equals("h2")) {
                INSTANCE = new H2Environment(profile);
            } else if (profile.profileName.equals("mssql")) {
                INSTANCE = new MsSqlEnvironment(profile);
            } else {
                logger.warn("getInstance: activeProfile={} not known," + " using generic profile", profile.profileName);
                INSTANCE = new DatabaseEnvironment(profile, new DatabaseConfig());
            }
        }

        return INSTANCE;
    }
}
