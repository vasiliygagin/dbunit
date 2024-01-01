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

import static org.junit.Assert.assertNotNull;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import java.sql.Connection;

import javax.sql.DataSource;

import org.dbunit.database.DatabaseDataSourceConnection;
import org.junit.Test;

public class TransactionAwareConnectionHelperTest {

    @Test
    public void shouldSupportNewConnection() throws Exception {
        DataSource dataSource = mock(DataSource.class);
        Connection connection = mock(Connection.class);
        given(dataSource.getConnection()).willReturn(connection);

        DatabaseDataSourceConnection databaseConnection = TransactionAwareConnectionHelper.newConnection(dataSource);

        assertNotNull(databaseConnection);
        databaseConnection.getConnection().createStatement();
        verify(dataSource).getConnection();
    }
}
