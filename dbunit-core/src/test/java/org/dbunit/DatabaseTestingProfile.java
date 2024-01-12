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
 * @author Vasiliy Gagin
 */
public class DatabaseTestingProfile {

    public final String driverClass;
    public final String connectionUrl;
    public final String schema;
    public final String user;
    public final String password;
    public final String profileDdl;
    public final boolean profileMultilineSupport;
    public final String[] unsupportedFeatures;

    public DatabaseTestingProfile(String driverClass, String connectionUrl, String schema, String user, String password,
            String profileDdl, boolean profileMultilineSupport, String[] unsupportedFeatures) {
        this.driverClass = driverClass;
        this.connectionUrl = connectionUrl;
        this.schema = schema;
        this.user = user;
        this.password = password;
        this.profileDdl = profileDdl;
        this.profileMultilineSupport = profileMultilineSupport;
        this.unsupportedFeatures = unsupportedFeatures;
    }

    public String getDriverClass() {
        return driverClass;
    }

    public String getConnectionUrl() {
        return connectionUrl;
    }

    public String getSchema() {
        return schema;
    }

    public String getUser() {
        return user;
    }

    public String getPassword() {
        return password;
    }

    public String getProfileDdl() {
        return profileDdl;
    }

    public boolean getProfileMultilineSupport() {
        return profileMultilineSupport;
    }

    public String[] getUnsupportedFeatures() {
        return unsupportedFeatures;
    }
}
