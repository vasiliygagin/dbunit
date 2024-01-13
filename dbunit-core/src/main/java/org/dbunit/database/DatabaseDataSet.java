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
import java.util.Set;

import org.dbunit.database.metadata.SchemaMetadata;
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

    private boolean allSchemasLoaded = false;
    private OrderedTableNameMap<ITableMetaData> tableMetaDatas = null;
    private final Set<SchemaMetadata> loadedSchemas = new HashSet<>();

    private final ITableFilterSimple _tableFilter;

    private ITable[] cachedTables;

    /**
     * Whether or not table names of this dataset are case sensitive. By default
     * case-sensitivity is set to false for datasets
     */
    private final boolean _caseSensitiveTableNames;

    /**
     * Creates a new database data set
     *
     * @param connection              The database connection
     * @throws SQLException
     * @since 2.4
     */
    public DatabaseDataSet(AbstractDatabaseConnection connection) throws SQLException {
        this(connection, null);
    }

    /**
     * Creates a new database data set
     *
     * @param connection              The database connection
     * @param tableFilter             Table filter to specify tables to be omitted
     *                                in this dataset. Can be <code>null</code>.
     * @throws SQLException
     * @since 2.4.3
     */
    public DatabaseDataSet(AbstractDatabaseConnection connection, ITableFilterSimple tableFilter) throws SQLException {
        if (connection == null) {
            throw new NullPointerException("The parameter 'connection' must not be null");
        }
        _caseSensitiveTableNames = connection.getDatabaseConfig().isCaseSensitiveTableNames();
        _connection = connection;
        defaultSchema = _connection.getSchema();
        config = connection.getDatabaseConfig();
        metadataHandler = config.getMetadataHandler();
        qualifiedTableNamesActive = config.isQualifiedTableNames();
        tableType = config.getTableTypes();
        _tableFilter = tableFilter;
    }

    private String qualifiedNameIfEnabled(String schemaName, String tableName) {
        QualifiedTableName qualifiedTableName = new QualifiedTableName(tableName, schemaName, null);
        return qualifiedTableName.getTableName(qualifiedTableNamesActive);
    }

    private void loadAllSchemas() throws DataSetException {
        if (allSchemasLoaded) {
            return;
        }

        if (tableMetaDatas == null) {
            tableMetaDatas = new OrderedTableNameMap<>(this._caseSensitiveTableNames);
        }

        SchemaMetadata schemaMetadata = null;
        if (defaultSchema != null) {
            schemaMetadata = _connection.getMetadataManager().findSchema(defaultSchema);
        }

        try {
            List<TableMetadata> tableMetadatas = _connection.getMetadataManager().getTables(schemaMetadata);
            for (TableMetadata tableMetadata : tableMetadatas) {

                String schemaName = tableMetadata.schemaMetadata.schema;
                String tableName = tableMetadata.tableName;

                if (_tableFilter != null && !_tableFilter.accept(tableName)) {
                    continue;
                }
                tableName = qualifiedNameIfEnabled(schemaName, tableName);

                // Put the table into the table map
                tableMetaDatas.add(tableName, null);
            }
            loadedSchemas.add(schemaMetadata);
        } catch (SQLException e) {
            throw new DataSetException(e);
        }
        allSchemasLoaded = true;
    }

    private void loadSchemaOfTable(String tableName1) throws DataSetException {
        if (allSchemasLoaded) {
            return;
        }
        // TODO: not sure if loading all schemas is too expensive. Or it might be so cheap that I should do all the time.
        QualifiedTableName2 tn = QualifiedTableName2.parseFullTableName2(tableName1, defaultSchema);
        String schema = tn.schema;
        if (schema == null || !qualifiedTableNamesActive) {
            loadAllSchemas();
            return;
        }

        SchemaMetadata schemaMetadata = _connection.getMetadataManager().findSchema(schema);

        if (tableMetaDatas != null && loadedSchemas.contains(schemaMetadata)) {
            return;
        }

        if (tableMetaDatas == null) {
            tableMetaDatas = new OrderedTableNameMap<>(this._caseSensitiveTableNames);
        }

        try {
            List<TableMetadata> tableMetadatas = _connection.getMetadataManager().getTables(schemaMetadata);
            for (TableMetadata tableMetadata : tableMetadatas) {

                String schemaName = tableMetadata.schemaMetadata.schema;
                String tableName = tableMetadata.tableName;

                if (_tableFilter != null && !_tableFilter.accept(tableName)) {
                    continue;
                }

                tableName = qualifiedNameIfEnabled(schemaName, tableName);

                // Put the table into the table map
                tableMetaDatas.add(tableName, null);
            }
            loadedSchemas.add(schemaMetadata);
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

    public final ITableMetaData getDatabaseTableMetaData(String tableName) throws DataSetException {
        return getTableMetaData(tableName);
    }

    @Override
    public final ITableMetaData getTableMetaData(String tableName) throws DataSetException {
        logger.debug("getTableMetaData(tableName={}) - start", tableName);

        loadSchemaOfTable(tableName);

        // Verify if table exist in the database
        if (!tableMetaDatas.containsTable(tableName)) {
            logger.error("Table '{}' not found in tableMap={}", tableName, tableMetaDatas);
            throw new NoSuchTableException(tableName);
        }

        // Try to find cached metadata
        ITableMetaData metaData = tableMetaDatas.get(tableName);
        if (metaData == null) {
            // Create metadata and cache it
            metaData = new DatabaseTableMetaData(tableName, _connection, isCaseSensitiveTableNames());
            // Put the metadata object into the cache map
            tableMetaDatas.update(tableName, metaData);
        }
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
