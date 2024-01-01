/*
 * Copyright 2002-2016 the original author or authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.springtestdbunit;

import java.sql.SQLException;
import java.util.Map;

import org.dbunit.database.IDatabaseConnection;

/**
 * Holds a number of {@link IDatabaseConnection} beans.
 *
 * @author Vasiliy Gagin
 */
public class DatabaseConnections {

    private final Map<String, IDatabaseConnection> connectionByName;
    private final String defaultName;

    public DatabaseConnections(Map<String, IDatabaseConnection> connectionByName, String defaultName) {
	this.connectionByName = connectionByName;
	this.defaultName = defaultName;
    }

    public void closeAll() throws SQLException {
	for (IDatabaseConnection connection : this.connectionByName.values()) {
	    connection.close();
	}
    }

    public IDatabaseConnection get(String name) {
	if (name == null || name.length() == 0) {
	    return defaultConnection();
	}
	IDatabaseConnection connection = connectionByName.get(name);
	if (connection == null) {
	    throw new IllegalStateException("Unable to find IDatabaseConnection named " + name);
	}
	return connection;
    }

    private IDatabaseConnection defaultConnection() {
	if (defaultName == null) {
	    throw new IllegalArgumentException(
		    "Requested a IDatabaseConnection without specifying name, but multiple connections available: "
			    + connectionByName.keySet() + ", Please provide connection name");
	}
	return connectionByName.get(defaultName);
    }
}
