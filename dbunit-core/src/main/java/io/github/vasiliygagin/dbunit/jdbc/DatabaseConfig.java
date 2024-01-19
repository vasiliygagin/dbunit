/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package io.github.vasiliygagin.dbunit.jdbc;

import org.dbunit.database.DefaultMetadataHandler;
import org.dbunit.database.ForwardOnlyResultSetTableFactory;
import org.dbunit.database.IMetadataHandler;
import org.dbunit.database.IResultSetTableFactory;
import org.dbunit.database.metadata.IgnoredTablePredicate;
import org.dbunit.database.statement.IStatementFactory;
import org.dbunit.database.statement.PreparedStatementFactory;
import org.dbunit.dataset.datatype.DefaultDataTypeFactory;
import org.dbunit.dataset.datatype.IDataTypeFactory;
import org.dbunit.dataset.filter.IColumnFilter;

/**
 *
 */
public class DatabaseConfig {

    private static final PreparedStatementFactory STATEMENT_FACTORY = new PreparedStatementFactory();
    private static final DefaultDataTypeFactory DATA_TYPE_FACTORY = new DefaultDataTypeFactory();
    private static final String[] TABLE_TYPES = { "TABLE" };
    private static final DefaultMetadataHandler METADATA_HANDLER = new DefaultMetadataHandler();

    private IResultSetTableFactory resultSetTableFactory = new ForwardOnlyResultSetTableFactory();
    private IStatementFactory statementFactory = STATEMENT_FACTORY;
    private IDataTypeFactory dataTypeFactory = DATA_TYPE_FACTORY;
    private String escapePattern = null;
    private String[] tableTypes = TABLE_TYPES;
    private IColumnFilter primaryKeysFilter = null;
    private int batchSize = 100;
    private int fetchSize = 100;
    private IMetadataHandler metadataHandler = METADATA_HANDLER;
    private IColumnFilter identityFilter = null;
    private boolean allowCountMismatch = false;
    private boolean caseSensitiveTableNames = false;
    private boolean qualifiedTableNames = false;
    private boolean batchedStatements = false;
    private boolean datatypeWarning = true;
    private boolean skipOracleRecycleBinTables = false;
    private boolean allowEmptyFields = false;
    private IgnoredTablePredicate ignoredTablePredicate = IgnoredTablePredicate.ALLOW_ALL;

    /**
     * Poor man's final
     */
    private boolean frozen = false;

    public DatabaseConfig() {
    }

    public final void freese() {
        frozen = true;
    }

    private void checkFrozen() {
        if (frozen) {
            throw new IllegalStateException();
        }
    }

    public IStatementFactory getStatementFactory() {
        return statementFactory;
    }

    public void setStatementFactory(IStatementFactory statementFactory) {
        checkFrozen();
        notNull(statementFactory, "statementFactory cannot be null");
        this.statementFactory = statementFactory;
    }

    public IResultSetTableFactory getResultSetTableFactory() {
        return resultSetTableFactory;
    }

    public void setResultSetTableFactory(IResultSetTableFactory resultSetTableFactory) {
        checkFrozen();
        this.resultSetTableFactory = resultSetTableFactory;
    }

    public IDataTypeFactory getDataTypeFactory() {
        return dataTypeFactory;
    }

    public void setDataTypeFactory(IDataTypeFactory dataTypeFactory) {
        checkFrozen();
        notNull(dataTypeFactory, "dataTypeFactory cannot be null");
        this.dataTypeFactory = dataTypeFactory;
    }

    public String getEscapePattern() {
        return escapePattern;
    }

    public void setEscapePattern(String escapePattern) {
        checkFrozen();
        this.escapePattern = escapePattern;
    }

    public String[] getTableTypes() {
        return tableTypes;
    }

    public void setTableTypes(String[] tableTypes) {
        checkFrozen();
        notNull(tableTypes, "tableTypes cannot be null");
        this.tableTypes = tableTypes;
    }

    public IColumnFilter getPrimaryKeysFilter() {
        return primaryKeysFilter;
    }

    public void setPrimaryKeysFilter(IColumnFilter primaryKeysFilter) {
        checkFrozen();
        this.primaryKeysFilter = primaryKeysFilter;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public void setBatchSize(int batchSize) {
        checkFrozen();
        this.batchSize = batchSize;
    }

    public int getFetchSize() {
        return fetchSize;
    }

    public void setFetchSize(int fetchSize) {
        checkFrozen();
        this.fetchSize = fetchSize;
    }

    public IMetadataHandler getMetadataHandler() {
        return metadataHandler;
    }

    public void setMetadataHandler(IMetadataHandler metadataHandler) {
        checkFrozen();
        notNull(metadataHandler, "metadataHandler cannot be null");
        this.metadataHandler = metadataHandler;
    }

    public IColumnFilter getIdentityFilter() {
        return identityFilter;
    }

    public void setIdentityFilter(IColumnFilter identityFilter) {
        checkFrozen();
        this.identityFilter = identityFilter;
    }

    public boolean isAllowCountMismatch() {
        return allowCountMismatch;
    }

    public void setAllowCountMismatch(boolean allowCountMismatch) {
        checkFrozen();
        this.allowCountMismatch = allowCountMismatch;
    }

    public boolean isCaseSensitiveTableNames() {
        return caseSensitiveTableNames;
    }

    public void setCaseSensitiveTableNames(boolean caseSensitiveTableNames) {
        checkFrozen();
        this.caseSensitiveTableNames = caseSensitiveTableNames;
    }

    public boolean isQualifiedTableNames() {
        return qualifiedTableNames;
    }

    public void setQualifiedTableNames(boolean qualifiedTableNames) {
        checkFrozen();
        this.qualifiedTableNames = qualifiedTableNames;
    }

    public boolean isBatchedStatements() {
        return batchedStatements;
    }

    public void setBatchedStatements(boolean batchedStatements) {
        checkFrozen();
        this.batchedStatements = batchedStatements;
    }

    public boolean isDatatypeWarning() {
        return datatypeWarning;
    }

    public void setDatatypeWarning(boolean datatypeWarning) {
        checkFrozen();
        this.datatypeWarning = datatypeWarning;
    }

    public boolean isSkipOracleRecycleBinTables() {
        return skipOracleRecycleBinTables;
    }

    public void setSkipOracleRecycleBinTables(boolean skipOracleRecycleBinTables) {
        checkFrozen();
        this.skipOracleRecycleBinTables = skipOracleRecycleBinTables;
    }

    public IgnoredTablePredicate getIgnoredTablePredicate() {
        return ignoredTablePredicate;
    }

    public void setIgnoredTablePredicate(IgnoredTablePredicate ignoredTablePredicate) {
        checkFrozen();
        this.ignoredTablePredicate = ignoredTablePredicate;
    }

    public boolean isAllowEmptyFields() {
        return allowEmptyFields;
    }

    public void setAllowEmptyFields(boolean allowEmptyFields) {
        checkFrozen();
        this.allowEmptyFields = allowEmptyFields;
    }

    private static void notNull(Object object, String message) {
        if (object == null) {
            throw new IllegalArgumentException(message);
        }
    }

    public void apply(DatabaseConfig source) {
        this.setStatementFactory(source.getStatementFactory());
        this.setResultSetTableFactory(source.getResultSetTableFactory());
        this.setDataTypeFactory(source.getDataTypeFactory());
        this.setEscapePattern(source.getEscapePattern());
        this.setTableTypes(source.getTableTypes());
        this.setPrimaryKeysFilter(source.getPrimaryKeysFilter());
        this.setBatchSize(source.getBatchSize());
        this.setFetchSize(source.getFetchSize());
        this.setMetadataHandler(source.getMetadataHandler());
        this.setIdentityFilter(source.getIdentityFilter());
        this.setCaseSensitiveTableNames(source.isCaseSensitiveTableNames());
        this.setQualifiedTableNames(source.isQualifiedTableNames());
        this.setBatchedStatements(source.isBatchedStatements());
        this.setDatatypeWarning(source.isDatatypeWarning());
        this.setSkipOracleRecycleBinTables(source.isSkipOracleRecycleBinTables());
        this.setIgnoredTablePredicate(source.getIgnoredTablePredicate());
        this.setAllowEmptyFields(source.isAllowEmptyFields());
        this.setAllowCountMismatch(source.isAllowCountMismatch());
    }
}
