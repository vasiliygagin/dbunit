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

import static java.util.Collections.singletonMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.BDDMockito.given;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import javax.sql.DataSource;

import org.dbunit.database.AbstractDatabaseConnection;
import org.dbunit.database.DatabaseDataSourceConnection;
import org.dbunit.dataset.IDataSet;
import org.dbunit.junit.internal.TestContextDriver;
import org.junit.Before;
import org.junit.Test;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.ContextLoader;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.util.ReflectionTestUtils;

import com.github.springtestdbunit.annotation.DatabaseOperation;
import com.github.springtestdbunit.annotation.DbUnitConfiguration;
import com.github.springtestdbunit.dataset.DataSetLoader;
import com.github.springtestdbunit.dataset.FlatXmlDataSetLoader;
import com.github.springtestdbunit.operation.DatabaseOperationLookup;
import com.github.springtestdbunit.operation.DefaultDatabaseOperationLookup;
import com.github.springtestdbunit.testutils.ExtendedTestContextManager;

/**
 * Tests for {@link DbUnitTestExecutionListener} prepare method.
 *
 * @author Phillip Webb
 */
public class DbUnitTestExecutionListenerPrepareTest {

    private static ThreadLocal<ApplicationContext> applicationContextThreadLocal = new ThreadLocal<>();

    private ApplicationContext applicationContext;

    private AbstractDatabaseConnection databaseConnection;

    private DataSource dataSource;

    @Before
    public void setup() {
        this.applicationContext = mock(ApplicationContext.class);
        this.databaseConnection = mock(AbstractDatabaseConnection.class);
        this.dataSource = mock(DataSource.class);
        DbUnitTestExecutionListenerPrepareTest.applicationContextThreadLocal.set(this.applicationContext);
    }

    @SuppressWarnings("unchecked")
    private void addBean(String beanName, Object bean) {
        given(this.applicationContext.containsBean(beanName)).willReturn(true);
        given(this.applicationContext.getBean(beanName)).willReturn(bean);
        given(this.applicationContext.getBean(eq(beanName), (Class) any())).willReturn(bean);
    }

    @Test
    public void shouldUseSensibleDefaultsOnClassWithNoDbUnitConfiguration() throws Exception {
        given(this.applicationContext.getBeansOfType(AbstractDatabaseConnection.class))
                .willReturn(singletonMap("dbUnitDatabaseConnection", this.databaseConnection));
        ExtendedTestContextManager testContextManager = new ExtendedTestContextManager(NoDbUnitConfiguration.class);
        testContextManager.prepareTestInstance();
        DbUnitTestExecutionListener listener = (DbUnitTestExecutionListener) testContextManager
                .getTestExecutionListeners().get(0);
        DatabaseConnections databaseConnections = listener.databaseConnections;
        assertSame(this.databaseConnection, databaseConnections.get("dbUnitDatabaseConnection"));
        assertEquals(FlatXmlDataSetLoader.class, listener.dataSetLoader.getClass());
        assertEquals(DefaultDatabaseOperationLookup.class, listener.databaseOperationLookup.getClass());

        TestContextDriver testContextDriver = (TestContextDriver) ReflectionTestUtils.getField(listener,
                "testContextDriver");
        testContextDriver.releaseTestContext();
    }

    @Test
    public void shouldTryBeanFactoryForCommonBeanNamesWithNoDbUnitConfiguration() throws Exception {
        testCommonBeanNames(NoDbUnitConfiguration.class);
    }

    @Test
    public void shouldTryBeanFactoryForCommonBeanNamesWithEmptyDatabaseConnection() throws Exception {
        testCommonBeanNames(EmptyDbUnitConfiguration.class);
    }

    private void testCommonBeanNames(Class<?> testClass) throws Exception {
        given(this.applicationContext.getBeansOfType(DataSource.class))
                .willReturn(singletonMap("dataSource", this.dataSource));
        given(this.applicationContext.getBeansOfType(AbstractDatabaseConnection.class))
                .willReturn(singletonMap("dbUnitDatabaseConnection", this.databaseConnection));
        ExtendedTestContextManager testContextManager = new ExtendedTestContextManager(testClass);
        testContextManager.prepareTestInstance();
        DbUnitTestExecutionListener listener = (DbUnitTestExecutionListener) testContextManager
                .getTestExecutionListeners().get(0);
        DatabaseConnections databaseConnections = listener.databaseConnections;
        assertSame(this.databaseConnection, databaseConnections.get("dbUnitDatabaseConnection"));
        verify(this.applicationContext).getBeansOfType(DataSource.class);
        verify(this.applicationContext).getBeansOfType(AbstractDatabaseConnection.class);
        verify(this.applicationContext).containsBean("dbUnitDataSetLoader");
        verifyNoMoreInteractions(this.applicationContext);

        TestContextDriver testContextDriver = (TestContextDriver) ReflectionTestUtils.getField(listener,
                "testContextDriver");
        testContextDriver.releaseTestContext();
    }

    @Test
    public void shouldConvertDatasetDatabaseConnection() throws Exception {
        given(this.applicationContext.getBeansOfType(DataSource.class))
                .willReturn(singletonMap("dataSource", this.dataSource));
        ExtendedTestContextManager testContextManager = new ExtendedTestContextManager(NoDbUnitConfiguration.class);
        testContextManager.prepareTestInstance();
        DbUnitTestExecutionListener listener = (DbUnitTestExecutionListener) testContextManager
                .getTestExecutionListeners().get(0);
        DatabaseConnections databaseConnections = listener.databaseConnections;
        Object connection = databaseConnections.get("dataSource");
        assertEquals(DatabaseDataSourceConnection.class, connection.getClass());

        TestContextDriver testContextDriver = (TestContextDriver) ReflectionTestUtils.getField(listener,
                "testContextDriver");
        testContextDriver.releaseTestContext();
    }

    @Test
    public void shouldFailIfNoDbConnectionBeanIsFound() throws Exception {
        ExtendedTestContextManager testContextManager = new ExtendedTestContextManager(NoDbUnitConfiguration.class);
        try {
            testContextManager.prepareTestInstance();
        } catch (IllegalStateException ex) {
            assertEquals(ex.getMessage(),
                    "No IDatabaseConenction found. Expecting at least one Spring bean of type org.dbunit.database.AbstractDatabaseConnection or javax.sql.DataSource.");
        }
        DbUnitTestExecutionListener listener = (DbUnitTestExecutionListener) testContextManager
                .getTestExecutionListeners().get(0);

        TestContextDriver testContextDriver = (TestContextDriver) ReflectionTestUtils.getField(listener,
                "testContextDriver");
        testContextDriver.releaseTestContext();
    }

    @Test
    public void shouldSupportAllDbUnitConfigurationAttributes() throws Exception {
        given(this.applicationContext.getBeansOfType(AbstractDatabaseConnection.class))
                .willReturn(singletonMap("customBean", this.databaseConnection));
        ExtendedTestContextManager testContextManager = new ExtendedTestContextManager(CustomConfiguration.class);
        testContextManager.prepareTestInstance();

        DbUnitTestExecutionListener listener = (DbUnitTestExecutionListener) testContextManager
                .getTestExecutionListeners().get(0);
        DatabaseConnections databaseConnections = listener.databaseConnections;
        assertSame(this.databaseConnection, databaseConnections.get("customBean"));
        assertEquals(CustomDataSetLoader.class, listener.dataSetLoader.getClass());
        assertEquals(CustomDatabaseOperationLookup.class, listener.databaseOperationLookup.getClass());

        TestContextDriver testContextDriver = (TestContextDriver) ReflectionTestUtils.getField(listener,
                "testContextDriver");
        testContextDriver.releaseTestContext();
    }

    @Test
    public void shouldFailIfDatasetLoaderCannotBeCreated() throws Exception {
        given(this.applicationContext.getBeansOfType(AbstractDatabaseConnection.class))
                .willReturn(singletonMap("dbUnitDatabaseConnection", this.databaseConnection));
        ExtendedTestContextManager testContextManager = new ExtendedTestContextManager(NonCreatableDataSetLoader.class);
        try {
            testContextManager.prepareTestInstance();
        } catch (IllegalArgumentException ex) {
            assertEquals("Unable to create data set loader instance for class "
                    + "com.github.springtestdbunit.DbUnitTestExecutionListenerPrepareTest$"
                    + "AbstractCustomDataSetLoader", ex.getMessage());
        }
        DbUnitTestExecutionListener listener = (DbUnitTestExecutionListener) testContextManager
                .getTestExecutionListeners().get(0);

        TestContextDriver testContextDriver = (TestContextDriver) ReflectionTestUtils.getField(listener,
                "testContextDriver");
        testContextDriver.releaseTestContext();
    }

    @Test
    public void shouldSupportCustomLoaderBean() throws Exception {
        given(this.applicationContext.getBeansOfType(DataSource.class))
                .willReturn(singletonMap("dataSource", this.dataSource));
        addBean("dbUnitDataSetLoader", new CustomDataSetLoader());
        ExtendedTestContextManager testContextManager = new ExtendedTestContextManager(EmptyDbUnitConfiguration.class);
        testContextManager.prepareTestInstance();
        DbUnitTestExecutionListener listener = (DbUnitTestExecutionListener) testContextManager
                .getTestExecutionListeners().get(0);
        assertEquals(CustomDataSetLoader.class, listener.dataSetLoader.getClass());

        TestContextDriver testContextDriver = (TestContextDriver) ReflectionTestUtils.getField(listener,
                "testContextDriver");
        testContextDriver.releaseTestContext();
    }

    private static class LocalApplicationContextLoader implements ContextLoader {

        @Override
        public String[] processLocations(Class<?> clazz, String... locations) {
            return new String[] {};
        }

        @Override
        public ApplicationContext loadContext(String... locations) throws Exception {
            return applicationContextThreadLocal.get();
        }
    }

    public abstract static class AbstractCustomDataSetLoader implements DataSetLoader {

        @Override
        public IDataSet loadDataSet(Class<?> testClass, String location) throws Exception {
            return null;
        }
    }

    public static class CustomDataSetLoader extends AbstractCustomDataSetLoader {
    }

    public static class CustomDatabaseOperationLookup implements DatabaseOperationLookup {

        @Override
        public org.dbunit.operation.DatabaseOperation get(DatabaseOperation operation) {
            return null;
        }
    }

    @ContextConfiguration(loader = LocalApplicationContextLoader.class)
    @TestExecutionListeners(DbUnitTestExecutionListener.class)
    private static class NoDbUnitConfiguration {

    }

    @ContextConfiguration(loader = LocalApplicationContextLoader.class)
    @TestExecutionListeners(DbUnitTestExecutionListener.class)
    @DbUnitConfiguration
    private static class EmptyDbUnitConfiguration {

    }

    @ContextConfiguration(loader = LocalApplicationContextLoader.class)
    @TestExecutionListeners(DbUnitTestExecutionListener.class)
    @DbUnitConfiguration(databaseConnection = "customBean", dataSetLoader = CustomDataSetLoader.class, databaseOperationLookup = CustomDatabaseOperationLookup.class)
    private static class CustomConfiguration {

    }

    @ContextConfiguration(loader = LocalApplicationContextLoader.class)
    @TestExecutionListeners(DbUnitTestExecutionListener.class)
    @DbUnitConfiguration(dataSetLoader = AbstractCustomDataSetLoader.class)
    private static class NonCreatableDataSetLoader {

    }

}
