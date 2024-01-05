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

import org.dbunit.database.DatabaseConfig;
import org.dbunit.database.IMetadataHandler;
import org.dbunit.database.IResultSetTableFactory;
import org.dbunit.database.statement.IStatementFactory;
import org.dbunit.dataset.datatype.IDataTypeFactory;
import org.dbunit.dataset.filter.IColumnFilter;
import org.springframework.util.Assert;

/**
 * A bean representation of the DB unit {@link DatabaseConfig} class. This bean
 * allows the database configuration from spring using standard property
 * arguments. The configuration from this bean can be {@link #apply applied} to
 * an existing {@link DatabaseConfig}.
 *
 * @author Phillip Webb
 */
public class DatabaseConfigBean {

    private io.github.vasiliygagin.dbunit.jdbc.DatabaseConfig databaseConfig = new io.github.vasiliygagin.dbunit.jdbc.DatabaseConfig();

    /**
     * Gets the statement factory database config property.
     *
     * @return the statement factory
     * @see DatabaseConfig#PROPERTY_STATEMENT_FACTORY
     */
    public IStatementFactory getStatementFactory() {
        return this.databaseConfig.getStatementFactory();
    }

    /**
     * Sets the statement factory database config property.
     *
     * @param statementFactory the statement factory
     * @see DatabaseConfig#PROPERTY_STATEMENT_FACTORY
     */
    public void setStatementFactory(IStatementFactory statementFactory) {
        this.databaseConfig.setStatementFactory(statementFactory);
    }

    /**
     * Gets the result set table factory database config property.
     *
     * @return the result set table factory
     * @see DatabaseConfig#PROPERTY_RESULTSET_TABLE_FACTORY
     */
    public IResultSetTableFactory getResultsetTableFactory() {
        return this.databaseConfig.getResultSetTableFactory();
    }

    /**
     * Sets the result set table factory database config property.
     *
     * @param resultSetTableFactory the result set table factory
     * @see DatabaseConfig#PROPERTY_RESULTSET_TABLE_FACTORY
     */
    public void setResultsetTableFactory(IResultSetTableFactory resultSetTableFactory) {
        this.databaseConfig.setResultSetTableFactory(resultSetTableFactory);
    }

    /**
     * Gets the data type factory database config property.
     *
     * @return the data type factory
     * @see DatabaseConfig#PROPERTY_DATATYPE_FACTORY
     */
    public IDataTypeFactory getDatatypeFactory() {
        return this.databaseConfig.getDataTypeFactory();
    }

    /**
     * Sets the data type factory database config property.
     *
     * @param dataTypeFactory the data type factory
     * @see DatabaseConfig#PROPERTY_DATATYPE_FACTORY
     */
    public void setDatatypeFactory(IDataTypeFactory dataTypeFactory) {
        this.databaseConfig.setDataTypeFactory(dataTypeFactory);
    }

    /**
     * Gets the escape pattern database config property.
     *
     * @return the escape pattern
     * @see DatabaseConfig#PROPERTY_ESCAPE_PATTERN
     */
    public String getEscapePattern() {
        return this.databaseConfig.getEscapePattern();
    }

    /**
     * Sets the escape pattern database config property.
     *
     * @param escapePattern the escape pattern
     * @see DatabaseConfig#PROPERTY_ESCAPE_PATTERN
     */
    public void setEscapePattern(String escapePattern) {
        this.databaseConfig.setEscapePattern(escapePattern);
    }

    /**
     * Gets the table type database config property.
     *
     * @return the table type
     * @see DatabaseConfig#PROPERTY_TABLE_TYPE
     */
    public String[] getTableType() {
        return this.databaseConfig.getTableTypes();
    }

    /**
     * Sets the table type database config property.
     *
     * @param tableTypes the table type
     * @see DatabaseConfig#PROPERTY_TABLE_TYPE
     */
    public void setTableType(String[] tableTypes) {
        this.databaseConfig.setTableTypes(tableTypes);
    }

    /**
     * Gets the primary key filter database config property.
     *
     * @return the primary key filter
     * @see DatabaseConfig#PROPERTY_PRIMARY_KEY_FILTER
     */
    public IColumnFilter getPrimaryKeyFilter() {
        return this.databaseConfig.getPrimaryKeysFilter();
    }

    /**
     * Sets the primary key filter database config property.
     *
     * @param primaryKeyFilter the primary key filter
     * @see DatabaseConfig#PROPERTY_PRIMARY_KEY_FILTER
     */
    public void setPrimaryKeyFilter(IColumnFilter primaryKeyFilter) {
        this.databaseConfig.setPrimaryKeysFilter(primaryKeyFilter);
    }

    /**
     * Gets the batch size database config property.
     *
     * @return the batch size
     * @see DatabaseConfig#PROPERTY_BATCH_SIZE
     */
    public Integer getBatchSize() {
        return this.databaseConfig.getBatchSize();
    }

    /**
     * Sets the batch size database config property.
     *
     * @param batchSize the batch size
     * @see DatabaseConfig#PROPERTY_BATCH_SIZE
     */
    public void setBatchSize(Integer batchSize) {
        Assert.notNull(batchSize, "batchSize cannot be null");
        this.databaseConfig.setBatchSize(batchSize);
    }

    /**
     * Gets the fetch size database config property.
     *
     * @return the fetch size
     * @see DatabaseConfig#PROPERTY_FETCH_SIZE
     */
    public Integer getFetchSize() {
        return this.databaseConfig.getFetchSize();
    }

    /**
     * Sets the fetch size database config property.
     *
     * @param fetchSize the fetch size
     * @see DatabaseConfig#PROPERTY_FETCH_SIZE
     */
    public void setFetchSize(Integer fetchSize) {
        Assert.notNull(fetchSize, "fetchSize cannot be null");
        this.databaseConfig.setFetchSize(fetchSize);
    }

    /**
     * Gets the meta-data handler database config property.
     *
     * @return the meta-data handler
     * @see DatabaseConfig#PROPERTY_METADATA_HANDLER
     */
    public IMetadataHandler getMetadataHandler() {
        return this.databaseConfig.getMetadataHandler();
    }

    /**
     * Sets the meta-data handler database config property.
     *
     * @param metadataHandler meta-data handler
     * @see DatabaseConfig#PROPERTY_METADATA_HANDLER
     */
    public void setMetadataHandler(IMetadataHandler metadataHandler) {
        this.databaseConfig.setMetadataHandler(metadataHandler);
    }

    /**
     * Gets the case sensitive table names database config feature.
     *
     * @return case sensitive table names
     * @see DatabaseConfig#FEATURE_CASE_SENSITIVE_TABLE_NAMES
     */
    public Boolean getCaseSensitiveTableNames() {
        return this.databaseConfig.isCaseSensitiveTableNames();
    }

    /**
     * Sets the case sensitive table names database config feature.
     *
     * @param caseSensitiveTableNames case sensitive table names
     * @see DatabaseConfig#FEATURE_CASE_SENSITIVE_TABLE_NAMES
     */
    public void setCaseSensitiveTableNames(Boolean caseSensitiveTableNames) {
        Assert.notNull(caseSensitiveTableNames, "caseSensitiveTableNames cannot be null");
        this.databaseConfig.setCaseSensitiveTableNames(caseSensitiveTableNames);
    }

    /**
     * Gets the qualified table names database config feature.
     *
     * @return the qualified table names
     * @see DatabaseConfig#FEATURE_QUALIFIED_TABLE_NAMES
     */
    public Boolean getQualifiedTableNames() {
        return this.databaseConfig.isQualifiedTableNames();
    }

    /**
     * Sets the qualified table names database config feature.
     *
     * @param qualifiedTableNames the qualified table names
     * @see DatabaseConfig#FEATURE_QUALIFIED_TABLE_NAMES
     */
    public void setQualifiedTableNames(Boolean qualifiedTableNames) {
        Assert.notNull(qualifiedTableNames, "qualifiedTableNames" + " cannot be null");
        this.databaseConfig.setQualifiedTableNames(qualifiedTableNames);
    }

    /**
     * Gets the batched statements database config feature.
     *
     * @return the batched statements
     * @see DatabaseConfig#FEATURE_BATCHED_STATEMENTS
     */
    public Boolean getBatchedStatements() {
        return this.databaseConfig.isBatchedStatements();
    }

    /**
     * Sets the batched statements database config feature.
     *
     * @param batchedStatements the batched statements
     * @see DatabaseConfig#FEATURE_BATCHED_STATEMENTS
     */
    public void setBatchedStatements(Boolean batchedStatements) {
        Assert.notNull(batchedStatements, "batchedStatements cannot be null");
        this.databaseConfig.setBatchedStatements(batchedStatements);
    }

    /**
     * Gets the datatype warning database config feature.
     *
     * @return the datatype warning
     * @see DatabaseConfig#FEATURE_DATATYPE_WARNING
     */
    public Boolean getDatatypeWarning() {
        return this.databaseConfig.isDatatypeWarning();
    }

    /**
     * Sets the datatype warning database config feature.
     *
     * @param datatypeWarning the datatype warning
     * @see DatabaseConfig#FEATURE_DATATYPE_WARNING
     */
    public void setDatatypeWarning(Boolean datatypeWarning) {
        Assert.notNull(datatypeWarning, "datatypeWarning cannot be null");
        this.databaseConfig.setDatatypeWarning(datatypeWarning);
    }

    /**
     * Gets the skip oracle recyclebin tables database config feature.
     *
     * @return the skip oracle recyclebin tables
     * @see DatabaseConfig#FEATURE_SKIP_ORACLE_RECYCLEBIN_TABLES
     */
    public Boolean getSkipOracleRecyclebinTables() {
        return this.databaseConfig.isSkipOracleRecycleBinTables();
    }

    /**
     * Sets the skip oracle recyclebin tables database config feature.
     *
     * @param skipOracleRecyclebinTables skip oracle recyclebin tables
     * @see DatabaseConfig#FEATURE_SKIP_ORACLE_RECYCLEBIN_TABLES
     */
    public void setSkipOracleRecyclebinTables(Boolean skipOracleRecyclebinTables) {
        Assert.notNull(skipOracleRecyclebinTables, "skipOracleRecyclebinTables cannot be null");
        this.databaseConfig.setSkipOracleRecycleBinTables(skipOracleRecyclebinTables);
    }

    /**
     * Gets the allow empty fields database config feature.
     *
     * @return the allow empty fields
     * @see DatabaseConfig#FEATURE_ALLOW_EMPTY_FIELDS
     */
    public Boolean getAllowEmptyFields() {
        return this.databaseConfig.isAllowEmptyFields();
    }

    /**
     * Sets the allow empty fields database config feature.
     *
     * @param allowEmptyFields allow empty fields
     * @see DatabaseConfig#FEATURE_ALLOW_EMPTY_FIELDS
     */
    public void setAllowEmptyFields(Boolean allowEmptyFields) {
        Assert.notNull(allowEmptyFields, "allowEmptyFields cannot be null");
        this.databaseConfig.setAllowEmptyFields(allowEmptyFields);
    }

    /**
     * Apply the configuration represented by this bean to the specified
     * databaseConfig.
     *
     * @param targetDatabaseConfig the database config to be updated.
     */
    void apply(io.github.vasiliygagin.dbunit.jdbc.DatabaseConfig targetDatabaseConfig) {
        targetDatabaseConfig.apply(this.databaseConfig);
    }

    public DatabaseConfig buildConfig() {
        DatabaseConfig config = new DatabaseConfig();
        apply(config);
        return config;
    }
}
