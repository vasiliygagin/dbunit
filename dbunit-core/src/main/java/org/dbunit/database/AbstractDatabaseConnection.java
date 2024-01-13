/*
 *
 * The DbUnit Database Testing Framework
 * Copyright (C)2002-2004, DbUnit.org
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 2.1 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, write to the Free Software
 * Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA
 *
 */

package org.dbunit.database;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Locale;

import org.dbunit.DatabaseUnitRuntimeException;
import org.dbunit.database.metadata.MetadataManager;
import org.dbunit.database.metadata.TableMetadata;
import org.dbunit.database.statement.IStatementFactory;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.FilteredDataSet;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.NoSuchTableException;
import org.dbunit.dataset.filter.SequenceTableFilter;
import org.dbunit.util.QualifiedTableName;
import org.dbunit.util.QualifiedTableName2;
import org.dbunit.util.SQLHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.vasiliygagin.dbunit.jdbc.DatabaseConfig;

/**
 * @author Manuel Laflamme
 * @version $Revision$
 * @since Mar 6, 2002
 */
public abstract class AbstractDatabaseConnection implements IDatabaseConnection {

    /**
     * Logger for this class
     */
    private static final Logger logger = LoggerFactory.getLogger(AbstractDatabaseConnection.class);

    protected final Connection jdbcConnection;
    private final MetadataManager metadataManager;
    private IDataSet _dataSet = null;
    final DatabaseConfig _databaseConfig;

    public AbstractDatabaseConnection(Connection jdbcConnection, DatabaseConfig config,
            MetadataManager metadataManager) {
        this.metadataManager = metadataManager;
        if (jdbcConnection == null) {
            throw new IllegalArgumentException("The parameter 'connection' must not be null");
        }
        this.jdbcConnection = jdbcConnection;
        _databaseConfig = config;
    }

    ////////////////////////////////////////////////////////////////////////////
    // IDatabaseConnection interface

    public MetadataManager getMetadataManager() {
        return metadataManager;
    }

    @Override
    public IDataSet createDataSet() throws SQLException {
        logger.debug("createDataSet() - start");

        if (_dataSet == null) {
            _dataSet = new DatabaseDataSet(this);
        }

        return _dataSet;
    }

    @Override
    public final Connection getConnection() throws SQLException {
        return jdbcConnection;
    }

    @Override
    public IDataSet createDataSet(String[] tableNames) throws DataSetException, SQLException {
        boolean caseSensitiveTableNames = _databaseConfig.isCaseSensitiveTableNames();
        SequenceTableFilter filter = new SequenceTableFilter(tableNames, caseSensitiveTableNames);
        return new FilteredDataSet(filter, createDataSet());
    }

    @Override
    public IResultSetTable createQueryTable(String resultName, String sql) throws DataSetException, SQLException {
        logger.debug("createQueryTable(resultName={}, sql={}) - start", resultName, sql);

        IResultSetTableFactory tableFactory = _databaseConfig.getResultSetTableFactory();
        IResultSetTable rsTable = tableFactory.createTable(resultName, sql, this);
        if (logger.isDebugEnabled()) {
            String rowCount = null;
            try {
                int rowCountInt = rsTable.getRowCount();
                rowCount = String.valueOf(rowCountInt);
            } catch (Exception e) {
                rowCount = "Unable to determine row count due to Exception: " + e.getLocalizedMessage();
            }
            logger.debug("createQueryTable: rowCount={}", rowCount);
        }
        return rsTable;
    }

    @Override
    public ITable createTable(String resultName, PreparedStatement preparedStatement)
            throws DataSetException, SQLException {
        logger.debug("createQueryTable(resultName={}, preparedStatement={}) - start", resultName, preparedStatement);

        IResultSetTableFactory tableFactory = _databaseConfig.getResultSetTableFactory();
        IResultSetTable rsTable = tableFactory.createTable(resultName, preparedStatement, this);
        return rsTable;
    }

    @Override
    public ITable createTable(String tableName) throws DataSetException, SQLException {
        logger.debug("createTable(tableName={}) - start", tableName);

        if (tableName == null) {
            throw new NullPointerException("The parameter 'tableName' must not be null");
        }

        String escapePattern = getDatabaseConfig().getEscapePattern();

        // qualify with schema if configured
        QualifiedTableName qualifiedTableName = new QualifiedTableName(tableName, this.getSchema(), escapePattern);
        String qualifiedName = qualifiedTableName.getQualifiedName();
        String sql = "select * from " + qualifiedName;
        return this.createQueryTable(tableName, sql);
    }

    @Override
    public int getRowCount(String tableName) throws SQLException {
        logger.debug("getRowCount(tableName={}) - start", tableName);

        return getRowCount(tableName, null);
    }

    @Override
    public int getRowCount(String tableName, String whereClause) throws SQLException {
        logger.debug("getRowCount(tableName={}, whereClause={}) - start", tableName, whereClause);

        StringBuffer sqlBuffer = new StringBuffer(128);
        sqlBuffer.append("select count(*) from ");

        // add table name and schema (schema only if available)
        QualifiedTableName qualifiedTableName = new QualifiedTableName(tableName, this.getSchema());
        String qualifiedName = qualifiedTableName.getQualifiedName();
        sqlBuffer.append(qualifiedName);
        if (whereClause != null) {
            sqlBuffer.append(" ");
            sqlBuffer.append(whereClause);
        }

        Statement statement = getConnection().createStatement();
        ResultSet resultSet = null;
        try {
            resultSet = statement.executeQuery(sqlBuffer.toString());
            if (resultSet.next()) {
                return resultSet.getInt(1);
            } else {
                throw new DatabaseUnitRuntimeException("Select count did not return any results for table '" + tableName
                        + "'. Statement: " + sqlBuffer.toString());
            }
        } finally {
            SQLHelper.close(resultSet, statement);
        }
    }

    @Override
    public DatabaseConfig getDatabaseConfig() {
        return _databaseConfig;
    }

    /**
     * @deprecated Use {@link #getConfig}
     */
    @Override
    @Deprecated
    public IStatementFactory getStatementFactory() {
        return _databaseConfig.getStatementFactory();
    }

    @Override
    public String toString() {
        StringBuffer sb = new StringBuffer();
        sb.append("_databaseConfig=").append(_databaseConfig);
        sb.append(", _dataSet=").append(_dataSet);
        return sb.toString();
    }

    public Object getMetadataStore() {
        return null;
    }

    protected TableMetadata toTableMetadata(String freeFormTableName) throws NoSuchTableException, DataSetException {
        // qualified names support - table name and schema is stored here
        QualifiedTableName2 tn = QualifiedTableName2.parseFullTableName2(freeFormTableName, getSchema());

        try {
            IMetadataHandler metadataHandler = getDatabaseConfig().getMetadataHandler();
            DatabaseMetaData databaseMetaData = getConnection().getMetaData();

            String catalog = metadataHandler.toCatalog(tn.schema);
            String schema = metadataHandler.toSchema(tn.schema);

            try ( //
                    ResultSet rs = databaseMetaData.getTables(catalog, schema, tn.table, null); //
            ) {
                while (rs.next()) {

                    TableMetadata tableMetadata = metadataManager.toTableMetadata(rs);
                    if (tableMetadata.tableName.equals(tn.table)) {
                        // JDBC uses underscore as a pattern character :-o. Wander whose smart idea that was.
                        return tableMetadata;
                    }
                }
                throw new NoSuchTableException("Did not find table '" + tn.table + "' in schema '" + tn.schema + "'");
            }
        } catch (SQLException e) {
            throw new DataSetException("Exception while validation existence of table '" + tn.table + "'", e);
        }

    }

    @Override
    public String correctTableName(String tableName) throws DataSetException {
        return oldWayCorrectTableName(tableName);
    }

    protected String oldWayCorrectTableName(String tableName) throws NoSuchTableException, DataSetException {
        // olg algorithm of correcting table names
        boolean caseSensitiveTableNames = _databaseConfig.isCaseSensitiveTableNames();
        boolean qualifiedTableNames = _databaseConfig.isQualifiedTableNames();

        String result1 = tableName;
        if (!caseSensitiveTableNames) {
            // "Locale.ENGLISH" Fixes bug #1537894 when clients have a special
            // locale like turkish. (for release 2.4.3)
            result1 = tableName.toUpperCase(Locale.ENGLISH);
        }
        String correctedCaseTableName = result1;
        try {
            boolean containsTable = false;
            List<TableMetadata> tableMetadatas = getMetadataManager().getTables(null);
            for (TableMetadata tableMetadata : tableMetadatas) {
                String qualifiedName;
                if (qualifiedTableNames) {
                    qualifiedName = tableMetadata.schemaMetadata.schema + "." + tableMetadata.tableName;
                } else {
                    qualifiedName = tableMetadata.tableName;
                }

                String result = qualifiedName;
                if (!caseSensitiveTableNames) {
                    // "Locale.ENGLISH" Fixes bug #1537894 when clients have a special
                    // locale like turkish. (for release 2.4.3)
                    result = qualifiedName.toUpperCase(Locale.ENGLISH);
                }

                if (result.equals(correctedCaseTableName)) {
                    containsTable = true;
                }
            }

            // Verify if table exist in the database
            if (!containsTable) {
                throw new NoSuchTableException(tableName);
            }

            if (!caseSensitiveTableNames) {
                return SQLHelper.correctCase(tableName, jdbcConnection);
            } else {
                return tableName;
            }
        } catch (SQLException exc) {
            throw new DataSetException(
                    "Exception while retrieving JDBC connection from dbunit connection '" + this + "'", exc);
        }
    }
}
