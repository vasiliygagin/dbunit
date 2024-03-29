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

import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.dbunit.database.DatabaseDataSourceConnection;
import org.dbunit.database.metadata.MetadataManager;
import org.springframework.jdbc.datasource.TransactionAwareDataSourceProxy;

import io.github.vasiliygagin.dbunit.jdbc.DatabaseConfig;

public class TransactionAwareConnectionHelper {

    public static TransactionAwareDataSourceProxy makeTransactionAware(DataSource dataSource) {
        if (dataSource instanceof TransactionAwareDataSourceProxy) {
            return (TransactionAwareDataSourceProxy) dataSource;
        }
        return new TransactionAwareDataSourceProxy(dataSource);
    }

    /**
     * Convenience method that can be used to construct a transaction aware
     * {@link DatabaseDataSourceConnection} from a {@link DataSource}.
     *
     * @param dataSource The data source
     * @return A {@link DatabaseDataSourceConnection}
     * @throws SQLException
     */
    public static DatabaseDataSourceConnection newConnection(DataSource dataSource) throws SQLException {
        DatabaseConfig config = new DatabaseConfig();
        Connection jdbcConnection = dataSource.getConnection();
        MetadataManager metadataManager = new MetadataManager(jdbcConnection, config, null, null);
        return new DatabaseDataSourceConnection(makeTransactionAware(dataSource), config, null, null, null,
                metadataManager);
    }
}
