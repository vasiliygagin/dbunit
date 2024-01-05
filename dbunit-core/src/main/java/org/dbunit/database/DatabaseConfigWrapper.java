/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.database;

import org.dbunit.database.statement.IStatementFactory;
import org.dbunit.dataset.datatype.IDataTypeFactory;
import org.dbunit.dataset.filter.IColumnFilter;

import io.github.vasiliygagin.dbunit.jdbc.DatabaseConfig;

/**
 * @deprecated Internal use only. To adapt new {@link DatabaseConfig} to legacy {@link org.dbunit.database.DatabaseConfig}
 */
@Deprecated
class DatabaseConfigWrapper extends org.dbunit.database.DatabaseConfig {

    private final DatabaseConfig delegate;

    public DatabaseConfigWrapper(DatabaseConfig delegate) {
        this.delegate = delegate;
    }

    @Override
    public IStatementFactory getStatementFactory() {
        return delegate.getStatementFactory();
    }

    @Override
    public void setStatementFactory(IStatementFactory statementFactory) {
        delegate.setStatementFactory(statementFactory);
    }

    @Override
    public IResultSetTableFactory getResultSetTableFactory() {
        return delegate.getResultSetTableFactory();
    }

    @Override
    public void setResultSetTableFactory(IResultSetTableFactory resultSetTableFactory) {
        delegate.setResultSetTableFactory(resultSetTableFactory);
    }

    @Override
    public IDataTypeFactory getDataTypeFactory() {
        return delegate.getDataTypeFactory();
    }

    @Override
    public void setDataTypeFactory(IDataTypeFactory dataTypeFactory) {
        delegate.setDataTypeFactory(dataTypeFactory);
    }

    @Override
    public String getEscapePattern() {
        return delegate.getEscapePattern();
    }

    @Override
    public void setEscapePattern(String escapePattern) {
        delegate.setEscapePattern(escapePattern);
    }

    @Override
    public String[] getTableTypes() {
        return delegate.getTableTypes();
    }

    @Override
    public void setTableTypes(String[] tableTypes) {
        delegate.setTableTypes(tableTypes);
    }

    @Override
    public IColumnFilter getPrimaryKeysFilter() {
        return delegate.getPrimaryKeysFilter();
    }

    @Override
    public void setPrimaryKeysFilter(IColumnFilter primaryKeysFilter) {
        delegate.setPrimaryKeysFilter(primaryKeysFilter);
    }

    @Override
    public int getBatchSize() {
        return delegate.getBatchSize();
    }

    @Override
    public void setBatchSize(int batchSize) {
        delegate.setBatchSize(batchSize);
    }

    @Override
    public int getFetchSize() {
        return delegate.getFetchSize();
    }

    @Override
    public void setFetchSize(int fetchSize) {
        delegate.setFetchSize(fetchSize);
    }

    @Override
    public IMetadataHandler getMetadataHandler() {
        return delegate.getMetadataHandler();
    }

    @Override
    public void setMetadataHandler(IMetadataHandler metadataHandler) {
        delegate.setMetadataHandler(metadataHandler);
    }

    @Override
    public IColumnFilter getIdentityFilter() {
        return delegate.getIdentityFilter();
    }

    @Override
    public void setIdentityFilter(IColumnFilter identityFilter) {
        delegate.setIdentityFilter(identityFilter);
    }

    @Override
    public boolean isAllowCountMismatch() {
        return delegate.isAllowCountMismatch();
    }

    @Override
    public void setAllowCountMismatch(boolean allowCountMismatch) {
        delegate.setAllowCountMismatch(allowCountMismatch);
    }

    @Override
    public boolean isCaseSensitiveTableNames() {
        return delegate.isCaseSensitiveTableNames();
    }

    @Override
    public void setCaseSensitiveTableNames(boolean caseSensitiveTableNames) {
        delegate.setCaseSensitiveTableNames(caseSensitiveTableNames);
    }

    @Override
    public boolean isQualifiedTableNames() {
        return delegate.isQualifiedTableNames();
    }

    @Override
    public void setQualifiedTableNames(boolean qualifiedTableNames) {
        delegate.setQualifiedTableNames(qualifiedTableNames);
    }

    @Override
    public boolean isBatchedStatements() {
        return delegate.isBatchedStatements();
    }

    @Override
    public void setBatchedStatements(boolean batchedStatements) {
        delegate.setBatchedStatements(batchedStatements);
    }

    @Override
    public boolean isDatatypeWarning() {
        return delegate.isDatatypeWarning();
    }

    @Override
    public void setDatatypeWarning(boolean datatypeWarning) {
        delegate.setDatatypeWarning(datatypeWarning);
    }

    @Override
    public boolean isSkipOracleRecycleBinTables() {
        return delegate.isSkipOracleRecycleBinTables();
    }

    @Override
    public void setSkipOracleRecycleBinTables(boolean skipOracleRecycleBinTables) {
        delegate.setSkipOracleRecycleBinTables(skipOracleRecycleBinTables);
    }

    @Override
    public boolean isAllowEmptyFields() {
        return delegate.isAllowEmptyFields();
    }

    @Override
    public void setAllowEmptyFields(boolean allowEmptyFields) {
        delegate.setAllowEmptyFields(allowEmptyFields);
    }
}
