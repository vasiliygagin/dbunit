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

import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.sql.DataSource;

import org.dbunit.database.AbstractDatabaseConnection;
import org.dbunit.database.DatabaseDataSourceConnection;
import org.dbunit.database.IDatabaseConnection;
import org.dbunit.junit.internal.TestContextAccessor;
import org.dbunit.junit.internal.TestContextDriver;
import org.dbunit.junit.internal.connections.SingleConnectionSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.TestContext;
import org.springframework.test.context.support.AbstractTestExecutionListener;
import org.springframework.test.context.transaction.TransactionalTestExecutionListener;
import org.springframework.util.StringUtils;

import com.github.springtestdbunit.annotation.DatabaseSetup;
import com.github.springtestdbunit.annotation.DatabaseTearDown;
import com.github.springtestdbunit.annotation.DbUnitConfiguration;
import com.github.springtestdbunit.annotation.ExpectedDatabase;
import com.github.springtestdbunit.dataset.DataSetLoader;
import com.github.springtestdbunit.dataset.FlatXmlDataSetLoader;
import com.github.springtestdbunit.operation.DatabaseOperationLookup;
import com.github.springtestdbunit.operation.DefaultDatabaseOperationLookup;

/**
 * <code>TestExecutionListener</code> which provides support for
 * {@link DatabaseSetup &#064;DatabaseSetup}, {@link DatabaseTearDown
 * &#064;DatabaseTearDown} and {@link ExpectedDatabase &#064;ExpectedDatabase}
 * annotations.
 * <p>
 * A bean named "<tt>dbUnitDatabaseConnection</tt>" or "<tt>dataSource</tt>" is
 * expected in the <tt>ApplicationContext</tt> associated with the test. This
 * bean can contain either a {@link IDatabaseConnection} or a {@link DataSource}
 * . A custom bean name can also be specified using the
 * {@link DbUnitConfiguration#databaseConnection() &#064;DbUnitConfiguration}
 * annotation.
 * <p>
 * Datasets are loaded using the {@link FlatXmlDataSetLoader} and DBUnit
 * database operation lookups are performed using the
 * {@link DefaultDatabaseOperationLookup} unless otherwise
 * {@link DbUnitConfiguration#dataSetLoader() configured}.
 * <p>
 * If you are running this listener in combination with the
 * {@link TransactionalTestExecutionListener} then pay attention to the order of
 * listeners. Specify {@link TransactionalTestExecutionListener} 1st so rollback
 * after test is executed last, after {@link DbUnitTestExecutionListener}.
 *
 * @author Vasiliy Gagin, Phillip Webb
 */
public class DbUnitTestExecutionListener extends AbstractTestExecutionListener {

    private static final Logger logger = LoggerFactory.getLogger(DbUnitTestExecutionListener.class);

    private static final String DATA_SET_LOADER_BEAN_NAME = "dbUnitDataSetLoader";

    private TestContextDriver testContextDriver;
    // TODO: probably want to save something in spring context for performance
    // reasons
    DataSetLoader dataSetLoader;
    DatabaseOperationLookup databaseOperationLookup;
    private DbUnitRunner runner = new DbUnitRunner();

    @Override
    public void prepareTestInstance(TestContext testContext) throws Exception {
        Class<?> testClass = testContext.getTestClass();
        ApplicationContext applicationContext = testContext.getApplicationContext();

        testContextDriver = TestContextAccessor.buildTestContext();

        loadStuff(testClass, applicationContext);
    }

    private void loadStuff(Class<?> testClass, ApplicationContext applicationContext) throws SQLException {
        String dataSetLoaderBeanName = null;
        Class<? extends DataSetLoader> dataSetLoaderClass = FlatXmlDataSetLoader.class;
        Class<? extends DatabaseOperationLookup> databaseOperationLookupClass = DefaultDatabaseOperationLookup.class;

        DbUnitConfiguration configuration = testClass.getAnnotation(DbUnitConfiguration.class);
        if (configuration != null) {
            dataSetLoaderClass = configuration.dataSetLoader();
            dataSetLoaderBeanName = configuration.dataSetLoaderBean();
            databaseOperationLookupClass = configuration.databaseOperationLookup();
        }

        prepareDatabaseConnections(applicationContext, configuration);
        dataSetLoader = prepareDataSetLoader(applicationContext, dataSetLoaderBeanName, dataSetLoaderClass);
        databaseOperationLookup = prepareDatabaseOperationLookup(databaseOperationLookupClass);
    }

    private void prepareDatabaseConnections(ApplicationContext applicationContext, DbUnitConfiguration configuration)
            throws SQLException {
        org.dbunit.junit.internal.TestContext dbunitTestContext = testContextDriver.getTestContext();

        Map<String, AbstractDatabaseConnection> allDatabaseConnections = discoverDatabaseConnections(
                applicationContext);
        if (allDatabaseConnections.isEmpty()) { // TODO not sure need to fail here
            throw new IllegalStateException("No IDatabaseConenction found. Expecting at least one Spring bean of type "
                    + AbstractDatabaseConnection.class.getName() + " or " + DataSource.class.getName() + ".");
        }

        Map<String, AbstractDatabaseConnection> selectedDatabaseConnections = null;
        List<String> names = discoverConfiguredConnectionNames(configuration);
        if (!names.isEmpty()) {
            selectedDatabaseConnections = filterByNames(allDatabaseConnections, names);
        } else {
            selectedDatabaseConnections = allDatabaseConnections;
        }

        if (configuration != null) {
            String defaultConnectionName = configuration.defaultConnectionName();
            if (!StringUtils.isEmpty(defaultConnectionName)) {
                dbunitTestContext.setDefaultConnectionName(defaultConnectionName);
            }
        }

        for (Entry<String, AbstractDatabaseConnection> entry : selectedDatabaseConnections.entrySet()) {
            String connectionName = entry.getKey();
            AbstractDatabaseConnection connection = entry.getValue();
            dbunitTestContext.addConnecionSource(connectionName, new SingleConnectionSource(connection));
        }
    }

    private List<String> discoverConfiguredConnectionNames(DbUnitConfiguration configuration) {
        List<String> names = new ArrayList<>();
        if (configuration != null) {
            String[] databaseConnectionBeanNames = configuration.databaseConnection();
            if (databaseConnectionBeanNames != null) {
                for (String name : databaseConnectionBeanNames) {
                    if (!StringUtils.isEmpty(name)) {
                        names.add(name);
                    }
                }
            }
        }
        return names;
    }

    private Map<String, AbstractDatabaseConnection> filterByNames(Map<String, AbstractDatabaseConnection> connections,
            List<String> names) {
        Map<String, AbstractDatabaseConnection> selectedConnections = new HashMap<>();
        for (String name : names) {
            AbstractDatabaseConnection connection = connections.get(name);
            if (connection == null) {
                throw new IllegalArgumentException(
                        "IDatabaseConenction can not be found. Expecting Spring bean of type "
                                + AbstractDatabaseConnection.class.getName() + " or " + DataSource.class.getName()
                                + " with name \"" + name + "\"");
            }
            selectedConnections.put(name, connection);
        }
        return selectedConnections;
    }

    private Map<String, AbstractDatabaseConnection> discoverDatabaseConnections(ApplicationContext applicationContext)
            throws SQLException {
        Map<String, AbstractDatabaseConnection> databaseConnections = new HashMap<>(
                applicationContext.getBeansOfType(AbstractDatabaseConnection.class));
        Map<String, DataSource> dataSources = applicationContext.getBeansOfType(DataSource.class);
        for (Entry<String, DataSource> entry : dataSources.entrySet()) {
            String beanName2 = entry.getKey();
            DataSource dataSource = entry.getValue();
            DatabaseDataSourceConnection databaseConnection = TransactionAwareConnectionHelper
                    .newConnection(dataSource);
            databaseConnections.put(beanName2, databaseConnection);
        }
        return databaseConnections;
    }

    private DataSetLoader prepareDataSetLoader(ApplicationContext applicationContext, String dataSetLoaderBeanName,
            Class<? extends DataSetLoader> dataSetLoaderClass) {
        if (!StringUtils.hasLength(dataSetLoaderBeanName)
                && applicationContext.containsBean(DATA_SET_LOADER_BEAN_NAME)) {
            dataSetLoaderBeanName = DATA_SET_LOADER_BEAN_NAME;
        }

        DataSetLoader dataSetLoader;
        if (StringUtils.hasLength(dataSetLoaderBeanName)) {
            if (logger.isDebugEnabled()) {
                logger.debug("DBUnit tests will load datasets using '" + dataSetLoaderBeanName + "'");
            }
            dataSetLoader = applicationContext.getBean(dataSetLoaderBeanName, DataSetLoader.class);
        } else {
            if (logger.isDebugEnabled()) {
                logger.debug("DBUnit tests will load datasets using " + dataSetLoaderClass);
            }
            try {
                dataSetLoader = dataSetLoaderClass.newInstance();
            } catch (Exception ex) {
                throw new IllegalArgumentException(
                        "Unable to create data set loader instance for " + dataSetLoaderClass, ex);
            }
        }
        return dataSetLoader;
    }

    private DatabaseOperationLookup prepareDatabaseOperationLookup(
            Class<? extends DatabaseOperationLookup> databaseOperationLookupClass) {
        DatabaseOperationLookup instance;
        try {
            instance = databaseOperationLookupClass.newInstance();
        } catch (Exception ex) {
            throw new IllegalArgumentException(
                    "Unable to create database operation lookup instance for " + databaseOperationLookupClass, ex);
        }
        return instance;
    }

    @Override
    public void beforeTestMethod(TestContext testContext) throws Exception {
        Class<?> testClass = testContext.getTestClass();
        Method testMethod = testContext.getTestMethod();
        testContextDriver.configureTestContext(testClass, testMethod);
        testContextDriver.beforeTest();
        org.dbunit.junit.internal.TestContext dbunitTestContext = testContextDriver.getTestContext();
        runner.beforeTestMethod(testClass, testMethod, dbunitTestContext, dataSetLoader, databaseOperationLookup);
    }

    @Override
    public void afterTestMethod(TestContext testContext) throws Exception {
        Class<?> testClass = testContext.getTestClass();
        Object testInstance = testContext.getTestInstance();
        Method testMethod = testContext.getTestMethod();
        Throwable testException = testContext.getTestException();

        org.dbunit.junit.internal.TestContext dbunitTestContext = testContextDriver.getTestContext();
        testException = runner.afterTestMethod(testClass, testInstance, testMethod, testException, dbunitTestContext,
                dataSetLoader, databaseOperationLookup);

        testContext.updateState(testInstance, testMethod, testException);
        testContextDriver.afterTest();
    }
}
