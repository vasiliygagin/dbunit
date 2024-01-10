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

import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;

import org.dbunit.database.metadata.TableMetadata;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.DataSetUtils;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.ITableIterator;
import org.dbunit.dataset.ITableMetaData;
import org.dbunit.dataset.NoSuchTableException;
import org.dbunit.dataset.OrderedTableNameMap;
import org.dbunit.dataset.filter.ITableFilterSimple;
import org.dbunit.util.QualifiedTableName;
import org.dbunit.util.QualifiedTableName2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.vasiliygagin.dbunit.jdbc.DatabaseConfig;

/**
 * Provides access to a database instance as a {@link IDataSet}.
 *
 * @author Manuel Laflamme
 * @author Last changed by: $Author$
 * @version $Revision$ $Date$
 * @since 1.0 (Feb 17, 2002)
 */
public class DatabaseDataSet implements IDataSet {

    /**
     * Logger for this class
     */
    private static final Logger logger = LoggerFactory.getLogger(DatabaseDataSet.class);

    private final AbstractDatabaseConnection _connection;
    private final String defaultSchema;
    private final DatabaseConfig config;
    private final boolean qualifiedTableNamesActive;
    private final String[] tableType;
    private final IMetadataHandler metadataHandler;

    private OrderedTableNameMap<ITableMetaData> tableMetaDatas = null;
    private final SchemaSet loadedSchemas;

    private final ITableFilterSimple _tableFilter;
    private final ITableFilterSimple _oracleRecycleBinTableFilter;

    private ITable[] cachedTables;

    /**
     * Whether or not table names of this dataset are case sensitive. By default
     * case-sensitivity is set to false for datasets
     */
    private boolean _caseSensitiveTableNames = false;

    /**
     * Creates a new database data set
     *
     * @param connection              The database connection
     * @param caseSensitiveTableNames Whether or not this dataset should use case
     *                                sensitive table names
     * @throws SQLException
     * @since 2.4
     */
    public DatabaseDataSet(AbstractDatabaseConnection connection, boolean caseSensitiveTableNames) throws SQLException {
        this(connection, caseSensitiveTableNames, null);
    }

    /**
     * Creates a new database data set
     *
     * @param connection              The database connection
     * @param caseSensitiveTableNames Whether or not this dataset should use case
     *                                sensitive table names
     * @param tableFilter             Table filter to specify tables to be omitted
     *                                in this dataset. Can be <code>null</code>.
     * @throws SQLException
     * @since 2.4.3
     */
    public DatabaseDataSet(AbstractDatabaseConnection connection, boolean caseSensitiveTableNames,
            ITableFilterSimple tableFilter) throws SQLException {
        _caseSensitiveTableNames = caseSensitiveTableNames;
        if (connection == null) {
            throw new NullPointerException("The parameter 'connection' must not be null");
        }
        _connection = connection;
        defaultSchema = _connection.getSchema();
        config = connection.getDatabaseConfig();
        metadataHandler = config.getMetadataHandler();
        qualifiedTableNamesActive = config.isQualifiedTableNames();
        tableType = config.getTableTypes();
        _tableFilter = tableFilter;
        _oracleRecycleBinTableFilter = new OracleRecycleBinTableFilter(config);
        loadedSchemas = new SchemaSet(caseSensitiveTableNames);
    }

    private String qualifiedNameIfEnabled(String schemaName, String tableName) {
        QualifiedTableName qualifiedTableName = new QualifiedTableName(tableName, schemaName, null);
        return qualifiedTableName.getTableName(qualifiedTableNamesActive);
    }

    private void loadAllSchemas() throws DataSetException {
        String schema = defaultSchema;
        if (!_caseSensitiveTableNames && schema != null) {
            // TODO rethink
            schema = schema.toUpperCase();
        }

        if (tableMetaDatas != null && loadedSchemas.contains(schema)) {
            return;
        }

        if (tableMetaDatas == null) {
            tableMetaDatas = new OrderedTableNameMap<>(this._caseSensitiveTableNames);
        }

        try {
            List<TableMetadata> tableMetadatas = _connection.getMetadataManager().getTables(null);
            for (TableMetadata tableMetadata : tableMetadatas) {

                String schemaName = tableMetadata.schemaMetadata.schema;
                String tableName = tableMetadata.tableName;

                if (_tableFilter != null && !_tableFilter.accept(tableName)) {
                    continue;
                }
                if (!_oracleRecycleBinTableFilter.accept(tableName)) {
                    logger.debug("Skipping oracle recycle bin table '{}'", tableName);
                    continue;
                }
                if (schema == null && !loadedSchemas.contains(schemaName)) {
                    loadedSchemas.add(schemaName);
                }

                tableName = qualifiedNameIfEnabled(schemaName, tableName);

                // Put the table into the table map
                tableMetaDatas.add(tableName, null);
            }
            loadedSchemas.add(schema);
        } catch (SQLException e) {
            throw new DataSetException(e);
        }
    }

    private void loadSchemaOfTable(String tableName1) throws DataSetException {
        if (tableName1 == null) {
            loadAllSchemas();
            return;
        }
        QualifiedTableName2 tn = QualifiedTableName2.parseFullTableName2(tableName1, defaultSchema);
        String schema = tn.schema;
        if (schema == null || !qualifiedTableNamesActive) {
            loadAllSchemas();
            return;
        }

        if (!_caseSensitiveTableNames) {
            // TODO rethink
            schema = schema.toUpperCase();
        }

        if (tableMetaDatas != null && loadedSchemas.contains(schema)) {
            return;
        }

        if (tableMetaDatas == null) {
            tableMetaDatas = new OrderedTableNameMap<>(this._caseSensitiveTableNames);
        }

        try {
            List<TableMetadata> tableMetadatas = _connection.getMetadataManager().getTables(schema);
            for (TableMetadata tableMetadata : tableMetadatas) {

                String schemaName = tableMetadata.schemaMetadata.schema;
                String tableName = tableMetadata.tableName;

                if (_tableFilter != null && !_tableFilter.accept(tableName)) {
                    continue;
                }
                if (!_oracleRecycleBinTableFilter.accept(tableName)) {
                    logger.debug("Skipping oracle recycle bin table '{}'", tableName);
                    continue;
                }
                if (schema == null && !loadedSchemas.contains(schemaName)) {
                    loadedSchemas.add(schemaName);
                }

                tableName = qualifiedNameIfEnabled(schemaName, tableName);

                // Put the table into the table map
                tableMetaDatas.add(tableName, null);
            }
            loadedSchemas.add(schema);
        } catch (SQLException e) {
            throw new DataSetException(e);
        }
    }

    private ITableIterator createIterator(boolean reversed) throws DataSetException {
        String[] names = getTableNames();
        if (reversed) {
            names = DataSetUtils.reverseStringArray(names);
        }

        return new DatabaseTableIterator(names, this);
    }

    ////////////////////////////////////////////////////////////////////////////
    // IDataSet interface

    @Override
    public String[] getTableNames() throws DataSetException {
        loadAllSchemas();

        return tableMetaDatas.getTableNames();
    }

    @Override
    public ITableMetaData getTableMetaData(String tableName) throws DataSetException {
        logger.debug("getTableMetaData(tableName={}) - start", tableName);

        loadSchemaOfTable(tableName);

        // Verify if table exist in the database
        if (!tableMetaDatas.containsTable(tableName)) {
            logger.error("Table '{}' not found in tableMap={}", tableName, tableMetaDatas);
            throw new NoSuchTableException(tableName);
        }

        // Try to find cached metadata
        ITableMetaData metaData = tableMetaDatas.get(tableName);
        if (metaData != null) {
            return metaData;
        }

        // Create metadata and cache it
        metaData = new DatabaseTableMetaData(tableName, _connection, true, isCaseSensitiveTableNames());
        // Put the metadata object into the cache map
        tableMetaDatas.update(tableName, metaData);

        return metaData;
    }

    @Override
    public ITable getTable(String tableName) throws DataSetException {
        logger.debug("getTable(tableName={}) - start", tableName);

        loadSchemaOfTable(tableName);

        try {
            ITableMetaData metaData = getTableMetaData(tableName);

            IResultSetTableFactory factory = config.getResultSetTableFactory();
            return factory.createTable(metaData, _connection);
        } catch (SQLException e) {
            throw new DataSetException(e);
        }
    }

    private static class SchemaSet {

        private final boolean isCaseSensitive;
        private final HashSet<String> set = new HashSet<>();

        private SchemaSet(boolean isCaseSensitive) {
            this.isCaseSensitive = isCaseSensitive;
        }

        public boolean contains(String schema) {
            return set.contains(normalizeSchema(schema));
        }

        public boolean add(String schema) {
            return set.add(normalizeSchema(schema));
        }

        private String normalizeSchema(String schema) {
            if (schema == null) {
                return null;
            } else if (!isCaseSensitive) {
                return schema.toUpperCase(Locale.ENGLISH);
            }
            return schema;
        }
    }

    private static class OracleRecycleBinTableFilter implements ITableFilterSimple {

        private final DatabaseConfig _config;

        public OracleRecycleBinTableFilter(DatabaseConfig config) {
            this._config = config;
        }

        @Override
        public boolean accept(String tableName) throws DataSetException {
            // skip oracle 10g recycle bin system tables if enabled
//            if (_config.isSkipOracleRecycleBinTables()) {
            if (_config.isSkipOracleRecycleBinTables()) {
                // Oracle 10g workaround
                // don't process system tables (oracle recycle bin tables) which
                // are reported to the application due a bug in the oracle JDBC driver
                if (tableName.startsWith("BIN$")) {
                    return false;
                }
            }

            return true;
        }
    }

    /**
     * @return <code>true</code> if the case sensitivity of table names is used in
     *         this dataset.
     * @since 2.4
     */
    @Override
    public boolean isCaseSensitiveTableNames() {
        return this._caseSensitiveTableNames;
    }

    @Override
    public ITable[] getTables() throws DataSetException {
        if (cachedTables == null) {

            // Gather all tables in the OrderedTableNameMap which also makes the duplicate
            // check
            OrderedTableNameMap<ITable> _orderedTableNameMap = new OrderedTableNameMap<>(this._caseSensitiveTableNames);
            loadAllSchemas();
            String[] names = tableMetaDatas.getTableNames();
            DatabaseTableIterator iterator = new DatabaseTableIterator(names, this);
            while (iterator.next()) {
                ITable table = iterator.getTable();
                _orderedTableNameMap.add(table.getTableMetaData().getTableName(), table);
            }
            cachedTables = _orderedTableNameMap.orderedValues().toArray(new ITable[0]);
        }

        return Arrays.copyOf(cachedTables, cachedTables.length); // Not sure why this has to be new array every time.
    }

    @Override
    public ITableIterator iterator() throws DataSetException {
        return createIterator(false);
    }

    @Override
    public ITableIterator reverseIterator() throws DataSetException {
        return createIterator(true);
    }
}
