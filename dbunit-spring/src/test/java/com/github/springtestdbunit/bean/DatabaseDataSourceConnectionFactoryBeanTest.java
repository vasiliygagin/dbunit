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

package com.github.springtestdbunit.bean;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.Connection;

import javax.sql.DataSource;

import org.dbunit.database.DatabaseConfig;
import org.dbunit.database.DatabaseDataSourceConnection;
import org.junit.Test;
import org.springframework.jdbc.datasource.TransactionAwareDataSourceProxy;

/**
 * Tests for {@link DatabaseDataSourceConnectionFactoryBean}.
 *
 * @author Phillip Webb
 */
public class DatabaseDataSourceConnectionFactoryBeanTest {

    private DatabaseDataSourceConnectionFactoryBean factoryBean = new DatabaseDataSourceConnectionFactoryBean();

    @Test
    public void shouldNotAllowObjectWithoutDataSet() throws Exception {
        try {
            this.factoryBean.getObject();
            fail();
        } catch (IllegalArgumentException ex) {
            assertEquals("The dataSource is required", ex.getMessage());
        }
    }

    @Test
    public void shouldCreateDatabaseDataSourceConnection() throws Exception {
        DataSource dataSource = mock(DataSource.class);
        Connection connection = mock(Connection.class);
        given(dataSource.getConnection()).willReturn(connection);
        this.factoryBean.setDataSource(dataSource);
        DatabaseDataSourceConnection bean = this.factoryBean.getObject();
        assertNotNull(bean);
        bean.getConnection().createStatement();
        verify(dataSource).getConnection();
    }

    @Test
    public void shouldAcceptUsernameAndPassword() throws Exception {
        DataSource dataSource = mock(DataSource.class);
        this.factoryBean.setDataSource(dataSource);
        this.factoryBean.setUsername("username");
        this.factoryBean.setPassword("password");

        Connection connection = mock(Connection.class);
        when(dataSource.getConnection("username", "password")).thenReturn(connection);

        DatabaseDataSourceConnection bean = this.factoryBean.getObject();
        assertNotNull(bean);

        bean.getConnection();
        verify(dataSource, times(2)).getConnection("username", "password");
    }

    @Test
    public void shouldSupportSchema() throws Exception {
        DataSource dataSource = mock(DataSource.class);
        this.factoryBean.setDataSource(dataSource);
        this.factoryBean.setSchema("schema");
        DatabaseDataSourceConnection bean = this.factoryBean.getObject();
        assertEquals("schema", bean.getSchema());
    }

    @Test
    public void shouldSupportDatabaseConfigBean() throws Exception {
        DataSource dataSource = mock(DataSource.class);
        this.factoryBean.setDataSource(dataSource);
        DatabaseConfigBean databaseConfigBean = mock(DatabaseConfigBean.class);
        this.factoryBean.setDatabaseConfig(databaseConfigBean);
        DatabaseConfig config = mock(DatabaseConfig.class);
        when(databaseConfigBean.buildConfig()).thenReturn(config);

        DatabaseDataSourceConnection bean = this.factoryBean.getObject();
        assertNotNull(bean);
        verify(databaseConfigBean).buildConfig();
    }

    @Test
    public void shouldBeSingleton() throws Exception {
        assertTrue(this.factoryBean.isSingleton());
    }

    @Test
    public void shouldBeCorrectClass() throws Exception {
        assertEquals(DatabaseDataSourceConnection.class, this.factoryBean.getObjectType());
    }

    @Test
    public void shouldCreateCreateTransactionAwareConnection() throws Exception {
        DataSource dataSource = mock(DataSource.class);
        this.factoryBean.setDataSource(dataSource);
        DatabaseDataSourceConnection dataSourceConnection = this.factoryBean.getObject();
        Connection connection = dataSourceConnection.getConnection();
        assertTrue(connection.toString() + " is not transaction aware",
                connection.toString().startsWith("Transaction-aware proxy"));
    }

    @Test
    public void shouldNotWrapCreateTransactionAwareConnection() throws Exception {
        DataSource dataSource = new TransactionAwareDataSourceProxy(mock(DataSource.class));
        this.factoryBean.setDataSource(dataSource);
        DatabaseDataSourceConnection dataSourceConnection = this.factoryBean.getObject();
        Connection connection = dataSourceConnection.getConnection();
        assertTrue(connection.toString() + " is not transaction aware", connection.toString()
                .startsWith("Transaction-aware proxy for target Connection  from DataSource [Mock for DataSource"));
    }

    @Test
    public void shouldRespectTransactionAwareAttribute() throws Exception {
        DataSource dataSource = mock(DataSource.class);
        Connection connection = mock(Connection.class);
        given(dataSource.getConnection()).willReturn(connection);
        this.factoryBean.setDataSource(dataSource);
        this.factoryBean.setTransactionAware(false);
        DatabaseDataSourceConnection dataSourceConnection = this.factoryBean.getObject();
        Connection actual = dataSourceConnection.getConnection();
        assertSame(connection, actual);
    }
}
