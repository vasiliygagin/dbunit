/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.database.metadata;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableSet;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import org.dbunit.database.IMetadataHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.github.vasiliygagin.dbunit.jdbc.DatabaseConfig;

/**
 *Responsible for loading and serving database metadata
 */
public class MetadataManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(MetadataManager.class);

    final Connection jdbcConnectione;
    final DatabaseConfig config;
    final IMetadataHandler metadataHandler;
    final String defaultCatalog;
    final String defaultSchema;

    // TODO: this needs to be coming from DB config
    private Set<SchemaMetadata> systemSchemas = new HashSet<>(
            asList(new SchemaMetadata(null, "INFORMATION_SCHEMA"), new SchemaMetadata(null, "SYSTEM_LOBS")));
    private Set<SchemaMetadata> allSchemas;
    private Map<SchemaMetadata, List<TableMetadata>> schemaTables = new HashMap<>();

    private String[] tableTypes;

    /**
     * @param config
     * @param defaultCatalog
     * @param defaultSchema
     * @param dataSource to use for accessing metadata
     */
    public MetadataManager(Connection jdbcConnectione, DatabaseConfig config, String defaultCatalog,
            String defaultSchema) {
        this.jdbcConnectione = jdbcConnectione;
        this.config = config;
        this.metadataHandler = config.getMetadataHandler();
        this.defaultCatalog = defaultCatalog;
        this.defaultSchema = defaultSchema;

        // TODO: should not load it here, instead once somewhere outside.
//        this.tableTypes = loadTableTypes(jdbcConnectione);
    }

    @FunctionalInterface
    public static interface MetaDataGet {

        ResultSet get(DatabaseMetaData databaseMetaData) throws SQLException;
    }

    private String[] loadTableTypes(Connection jdbcConnectione) {
        return loadSingleColumnDataAsArray(jdbcConnectione, DatabaseMetaData::getTableTypes, null);
    }

    private String[] loadSingleColumnDataAsArray(Connection jdbcConnectione, MetaDataGet get,
            Predicate<String> filter) {
        List<String> tableTypes = loadSingleColumnData(jdbcConnectione, get, filter);
        return tableTypes.toArray(new String[tableTypes.size()]);
    }

    private List<String> loadSingleColumnData(Connection jdbcConnectione, MetaDataGet get, Predicate<String> filter) {
        List<String> tableTypes = new ArrayList<>();
        try {
            DatabaseMetaData databaseMetaData = jdbcConnectione.getMetaData();
            ResultSet rs = get.get(databaseMetaData);
            while (rs.next()) {
                tableTypes.add(rs.getString(1));
            }
            rs.close();
            if (filter != null) {
                tableTypes.removeIf(filter);
            }
        } catch (SQLException exc) {
            throw new RuntimeException(exc);
        }
        return tableTypes;
    }

    public DatabaseMetaData getMetaData() throws SQLException {
        return jdbcConnectione.getMetaData();
    }

    // TODO: want to get catalog schema mess out of here?
    public List<TableMetadata> getTables(String catalogSchema) throws SQLException {
        String catalog = metadataHandler.toCatalog(catalogSchema);
        String schema = metadataHandler.toSchema(catalogSchema);

        return getTables(catalog, schema);
    }

    public List<TableMetadata> getTables(String catalog, String schema) throws SQLException {
        if (catalog == null && schema == null) {
            LOGGER.warn("Whole database metadata requested, could be very expensive");
            Set<SchemaMetadata> schemasToLoad = new HashSet<>(getAllSchemas());
            schemasToLoad.removeAll(schemaTables.keySet());

            List<TableMetadata> result = new ArrayList<>();
            for (SchemaMetadata schemaMetadata : schemasToLoad) {
                result.addAll(loadSchemaTables(schemaMetadata));
            }
            return result;
        }

        SchemaMetadata schemaMetadata = new SchemaMetadata(catalog, schema);
        List<TableMetadata> tableMetadatas = schemaTables.get(schemaMetadata);
        if (tableMetadatas != null) {
            return tableMetadatas;
        }

        return loadSchemaTables(schemaMetadata);
    }

    /**
     * @param resultSet
     * @return
     * @throws SQLException
     */
    private TableMetadata toTableMetadata(ResultSet resultSet) throws SQLException {
        String catalog = resultSet.getString(1);
        String schema = resultSet.getString(2);
        String tableName = resultSet.getString(3);
        String tableType = resultSet.getString(4);

        SchemaMetadata schemaMetadata = new SchemaMetadata(catalog, schema);
        return new TableMetadata(schemaMetadata, tableName, tableType);
    }

    /**
     * @param schema
     * @return
     * @throws SQLException
     */
    private List<TableMetadata> loadSchemaTables(SchemaMetadata schema) throws SQLException {
        DatabaseMetaData databaseMetaData = jdbcConnectione.getMetaData();
        String[] tableTypes = config.getTableTypes();
        ResultSet resultSet = databaseMetaData.getTables(schema.catalog, schema.schema, "%", tableTypes);

        List<TableMetadata> tableMetadatas = new ArrayList<>();
        try {
            while (resultSet.next()) {
                TableMetadata tableMetadata = toTableMetadata(resultSet);
                if (systemSchemas.contains(tableMetadata.schemaMetadata)) {
                    continue;
                }
                tableMetadatas.add(tableMetadata);
            }
        } finally {
            resultSet.close();
        }

        schemaTables.put(schema, tableMetadatas);
        return tableMetadatas;
    }

    private Set<SchemaMetadata> getAllSchemas() {
        if (allSchemas == null) {
            Set<SchemaMetadata> schemas = new HashSet<>();
            try {
                DatabaseMetaData databaseMetaData = jdbcConnectione.getMetaData();
                ResultSet rs = databaseMetaData.getSchemas();
                while (rs.next()) {
                    SchemaMetadata schema = new SchemaMetadata(rs.getString(2), rs.getString(1));
                    if (systemSchemas.contains(schema)) {
                        continue;
                    }
                    schemas.add(schema);
                }
                rs.close();
            } catch (SQLException exc) {
                throw new RuntimeException(exc);
            }
            allSchemas = unmodifiableSet(schemas);
        }
        return allSchemas;
    }
}
