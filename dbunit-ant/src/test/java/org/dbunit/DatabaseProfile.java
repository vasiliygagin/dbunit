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

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.StringTokenizer;

/**
 * @author Vasiliy Gagin
 */
public class DatabaseProfile {

    private static final String[] EMPTY_ARRAY = {};

    private static final String PROFILE_URL = "dbunit.profile.url";
    private static final String PROFILE_SCHEMA = "dbunit.profile.schema";
    private static final String PROFILE_USER = "dbunit.profile.user";
    private static final String PROFILE_PASSWORD = "dbunit.profile.password";
    private static final String PROFILE_UNSUPPORTED_FEATURES = "dbunit.profile.unsupportedFeatures";
    private static final String PROFILE_DDL = "dbunit.profile.ddl";
    private static final String PROFILE_MULTILINE_SUPPORT = "dbunit.profile.multiLineSupport";

    public final String profileName = "hsqldb";
    private final Properties _properties;

    public DatabaseProfile() {
        final Properties properties = System.getProperties();
        properties.put("dbunit.profile", "hsqldb");
        properties.put("dbunit.profile.driverClass", "org.hsqldb.jdbcDriver");
        properties.put("dbunit.profile.url", "jdbc:hsqldb:mem:.");
        properties.put("dbunit.profile.schema", "PUBLIC");
        properties.put("dbunit.profile.user", "sa");
        properties.put("dbunit.profile.password", "");
        properties.put("dbunit.profile.ddl", "hypersonic.sql");
        properties.put("dbunit.profile.unsupportedFeatures",
                "BLOB,CLOB,SCROLLABLE_RESULTSET,INSERT_IDENTITY,TRUNCATE_TABLE,SDO_GEOMETRY,XML_TYPE");
        properties.put("dbunit.profile.multiLineSupport", "true");
        _properties = properties;
    }

    public String getDriverClass() {
        return "org.hsqldb.jdbcDriver";
    }

    public String getConnectionUrl() {
        return _properties.getProperty(PROFILE_URL);
    }

    public String getSchema() {
        return _properties.getProperty(PROFILE_SCHEMA, null);
    }

    public String getUser() {
        return _properties.getProperty(PROFILE_USER);
    }

    public String getPassword() {
        return _properties.getProperty(PROFILE_PASSWORD);
    }

    public String getProfileDdl() {
        return _properties.getProperty(PROFILE_DDL);
    }

    public boolean getProfileMultilineSupport() {
        return Boolean.valueOf(_properties.getProperty(PROFILE_MULTILINE_SUPPORT));
    }

    public String[] getUnsupportedFeatures() {
        String property = _properties.getProperty(PROFILE_UNSUPPORTED_FEATURES);

        // If property is not set return an empty array
        if (property == null) {
            return EMPTY_ARRAY;
        }

        List<String> stringList = new ArrayList<>();
        StringTokenizer tokenizer = new StringTokenizer(property, ",");
        while (tokenizer.hasMoreTokens()) {
            stringList.add(tokenizer.nextToken().trim());
        }
        return stringList.toArray(new String[stringList.size()]);
    }
}
