/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.database;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import org.dbunit.database.metadata.MetadataManager;
import org.dbunit.database.metadata.SchemaMetadata;
import org.dbunit.database.metadata.TableFinder;
import org.dbunit.database.metadata.TableMetadata;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.DataSetUtils;
import org.dbunit.dataset.IDataSet;
import org.dbunit.dataset.ITable;
import org.dbunit.dataset.ITableIterator;
import org.dbunit.dataset.ITableMetaData;
import org.dbunit.dataset.NoSuchTableException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.vasiliygagin.dbunit.jdbc.DatabaseConfig;

public class DatabaseDataSet implements IDataSet {

    /**
     * Logger for this class
     */
    private static final Logger logger = LoggerFactory.getLogger(DatabaseDataSet.class);

    private final AbstractDatabaseConnection _connection;
    private final String defaultSchema;
    private final DatabaseConfig config;
    private final boolean qualifiedTableNamesActive;

    private final Map<TableMetadata, DatabaseTableMetaData> tableMetaDatas;

    private final Predicate<String> acceptedTable;
    private final TableFinder tableFinder;

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
     * @throws DataSetException
     * @since 2.4
     */
    public DatabaseDataSet(AbstractDatabaseConnection connection, TableFinder tableFinder)
            throws SQLException, DataSetException {
        this(connection, tableFinder, tableName -> true);
    }

    public DatabaseDataSet(AbstractDatabaseConnection connection, TableFinder tableFinder,
            Predicate<String> acceptedTable) throws DataSetException {
        this.acceptedTable = acceptedTable;
        this.tableFinder = tableFinder;
        _caseSensitiveTableNames = connection.getDatabaseConfig().isCaseSensitiveTableNames();
        _connection = connection;
        defaultSchema = _connection.getSchema();
        config = connection.getDatabaseConfig();
        qualifiedTableNamesActive = config.isQualifiedTableNames();
        tableMetaDatas = new LinkedHashMap<>();

        MetadataManager metadataManager = _connection.getMetadataManager();
        SchemaMetadata schemaMetadata = null;
        if (defaultSchema != null) {
            schemaMetadata = metadataManager.findSchema(defaultSchema);
        }

        try {
            List<TableMetadata> tableMetadatas = metadataManager.getTables(schemaMetadata);
            for (TableMetadata tableMetadata : tableMetadatas) {
                if (acceptedTable.test(tableMetadata.tableName)) {
                    DatabaseTableMetaData metaData = new DatabaseTableMetaData(tableMetadata.tableName, _connection,
                            isCaseSensitiveTableNames());
                    tableMetaDatas.put(tableMetadata, metaData);
                }
            }
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
    @Deprecated
    public String[] getTableNames() throws DataSetException {
        List<String> tableNames = tableMetaDatas.keySet().stream().map(tm -> tm.tableName).collect(Collectors.toList());
        return tableNames.toArray(new String[0]);
    }

    public final ITableMetaData getDatabaseTableMetaData(String tableName) throws DataSetException {
        return getTableMetaData(tableName);
    }

    @Override
    public final ITableMetaData getTableMetaData(String tableName) throws DataSetException {
        TableMetadata tableMetadata = tableFinder.nameToTable(tableName);
        // Verify if table exist in the database
        if (!tableMetaDatas.containsKey(tableMetadata)) {
            logger.error("Table '{}' not found in tableMap={}", tableName, tableMetaDatas);
            throw new NoSuchTableException(tableName);
        }

        // Try to find cached metadata
        DatabaseTableMetaData metaData = tableMetaDatas.get(tableMetadata);
        if (metaData == null) {
            // Create metadata and cache it
            metaData = new DatabaseTableMetaData(tableName, _connection, isCaseSensitiveTableNames());
            // Put the metadata object into the cache map
            tableMetaDatas.put(tableMetadata, metaData);
        }
        return metaData;
    }

    @Override
    public ITable getTable(String tableName) throws DataSetException {
        CachedResultSetTable cachedResultSetTable;
        try {
            ITableMetaData metaData = getTableMetaData(tableName);

            IResultSetTableFactory resultSetTableFactory = config.getResultSetTableFactory();
            IResultSetTable resultSetTable = resultSetTableFactory.createTable(metaData, _connection);
            cachedResultSetTable = new CachedResultSetTable(resultSetTable);
        } catch (SQLException e) {
            throw new DataSetException(e);
        }
        return cachedResultSetTable;
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
        List<ITable> _orderedTableNameMap = new ArrayList<>();
        for (Entry<TableMetadata, DatabaseTableMetaData> entry : tableMetaDatas.entrySet()) {
            DatabaseTableMetaData metaData = entry.getValue();
            _connection.loadTableResultSet(entry.getKey().tableName);
            CachedResultSetTable cachedResultSetTable;
            try {
                ForwardOnlyResultSetTable resultSetTable = new ForwardOnlyResultSetTable(metaData, _connection);
                cachedResultSetTable = new CachedResultSetTable(resultSetTable);
            } catch (SQLException e) {
                throw new DataSetException(e);
            }
            _orderedTableNameMap.add(cachedResultSetTable);
            cachedResultSetTable.close();
        }
        return _orderedTableNameMap.toArray(new ITable[0]);
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
