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

import java.util.Map;
import java.util.Map.Entry;

import org.dbunit.database.AbstractDatabaseConnection;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.junit.internal.TestContext;
import org.dbunit.junit.internal.connections.SingleConnectionSource;

/**
 * Holds a number of {@link IDatabaseConnection} beans.
 *
 * @author Vasiliy Gagin
 */
public class DatabaseConnections {

    public DatabaseConnections(Map<String, AbstractDatabaseConnection> connectionByName, String defaultName,
            TestContext dbunitTestContext) {
        init(connectionByName, defaultName, dbunitTestContext);
    }

    static void init(Map<String, AbstractDatabaseConnection> connectionByName, String defaultName,
            TestContext dbunitTestContext) {
        dbunitTestContext.setDefaultConnectionName(defaultName);

        for (Entry<String, AbstractDatabaseConnection> entry : connectionByName.entrySet()) {
            String connectionName = entry.getKey();
            AbstractDatabaseConnection connection = entry.getValue();
            dbunitTestContext.addConnecionSource(connectionName, new SingleConnectionSource(connection));
        }
    }
}
