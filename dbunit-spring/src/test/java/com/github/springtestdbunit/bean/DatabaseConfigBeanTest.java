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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;

import org.dbunit.database.DefaultMetadataHandler;
import org.dbunit.database.IMetadataHandler;
import org.dbunit.database.IResultSetTableFactory;
import org.dbunit.database.statement.IStatementFactory;
import org.dbunit.dataset.datatype.IDataTypeFactory;
import org.dbunit.dataset.filter.IColumnFilter;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.BeanWrapper;
import org.springframework.beans.BeanWrapperImpl;

import io.github.vasiliygagin.dbunit.jdbc.DatabaseConfig;

/**
 * Tests for {@link DatabaseConfigBean}.
 *
 * @author Phillip Webb
 */
public class DatabaseConfigBeanTest {

    private static final Set<Class<?>> CLASS_COMPARE_ONLY;

    static {
        CLASS_COMPARE_ONLY = new HashSet<>();
        CLASS_COMPARE_ONLY.add(DefaultMetadataHandler.class);
    }

    private DatabaseConfig defaultConfig = new DatabaseConfig();

    private DatabaseConfigBean configBean;

    private BeanWrapper configBeanWrapper;

    @Before
    public void setup() {
        this.configBean = new DatabaseConfigBean();
        this.configBeanWrapper = new BeanWrapperImpl(this.configBean);
    }

    @Test
    public void shouldAllowSetOfNonMandatoryFieldToNull() throws Exception {
        this.configBean.setPrimaryKeyFilter(null);
    }

    @Test
    public void shouldFailWhenSetingMandatoryFieldToNull() throws Exception {
        try {
            this.configBean.setDatatypeFactory(null);
            fail();
        } catch (IllegalArgumentException ex) {
            assertEquals("dataTypeFactory cannot be null", ex.getMessage());
        }
    }

    @Test
    public void testStatementFactory() throws Exception {
        doTest("statementFactory", DatabaseConfig::getStatementFactory, mock(IStatementFactory.class));
    }

    @Test
    public void testResultsetTableFactory() {
        doTest("resultsetTableFactory", DatabaseConfig::getResultSetTableFactory, mock(IResultSetTableFactory.class));
    }

    @Test
    public void testDatatypeFactory() {
        doTest("datatypeFactory", DatabaseConfig::getDataTypeFactory, mock(IDataTypeFactory.class));
    }

    @Test
    public void testEscapePattern() {
        doTest("escapePattern", DatabaseConfig::getEscapePattern, "test");
    }

    @Test
    public void testTableType() {
        doTest("tableType", DatabaseConfig::getTableTypes, new String[] { "test" });
    }

    @Test
    public void testPrimaryKeyFilter() {
        doTest("primaryKeyFilter", DatabaseConfig::getPrimaryKeysFilter, mock(IColumnFilter.class));
    }

    @Test
    public void testBatchSize() {
        doTest("batchSize", DatabaseConfig::getBatchSize, new Integer(123));
    }

    @Test
    public void testFetchSize() {
        doTest("fetchSize", DatabaseConfig::getFetchSize, new Integer(123));
    }

    @Test
    public void testMetadataHandler() {
        doTest("metadataHandler", DatabaseConfig::getMetadataHandler, mock(IMetadataHandler.class));
    }

    @Test
    public void testCaseSensitiveTableNames() {
        doTest("caseSensitiveTableNames", DatabaseConfig::isCaseSensitiveTableNames, Boolean.TRUE);
    }

    @Test
    public void testQualifiedTableNames() {
        doTest("qualifiedTableNames", DatabaseConfig::isQualifiedTableNames, Boolean.TRUE);
    }

    @Test
    public void testBatchedStatements() {
        doTest("batchedStatements", DatabaseConfig::isBatchedStatements, Boolean.TRUE);
    }

    @Test
    public void testDatatypeWarning() {
        doTest("datatypeWarning", DatabaseConfig::isDatatypeWarning, Boolean.FALSE);
    }

    @Test
    public void testSkipOracleRecyclebinTables() {
        doTest("skipOracleRecyclebinTables", DatabaseConfig::isSkipOracleRecycleBinTables, Boolean.TRUE);
    }

    private <T> void doTest(String propertyName, Function<DatabaseConfig, T> getter, T newValue) {
        T initialValue = (T) this.configBeanWrapper.getPropertyValue(propertyName);

        assertFalse("Unable to test if new value is same as intial value", newValue.equals(initialValue));
        this.configBeanWrapper.setPropertyValue(propertyName, newValue);
        DatabaseConfig appliedConfig = this.configBean.buildConfig();

        assertEquals("Did not replace " + propertyName + " value", newValue, getter.apply(appliedConfig));
    }
}
