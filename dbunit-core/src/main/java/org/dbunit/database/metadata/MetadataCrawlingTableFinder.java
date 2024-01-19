/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.database.metadata;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.dbunit.database.AmbiguousTableNameException;
import org.dbunit.dataset.DataSetException;
import org.dbunit.dataset.NoSuchTableException;

/**
 *
 */
public class MetadataCrawlingTableFinder implements TableFinder {

    private final MetadataManager metadataManager;

    public MetadataCrawlingTableFinder(MetadataManager metadataManager) {
        this.metadataManager = metadataManager;
    }

    @Override
    public TableMetadata nameToTable(String freeHandTableName) throws NoSuchTableException, DataSetException {
        String[] parts = freeHandTableName.split("\\.");
        if (parts.length > 3) {
            throw new DataSetException("Invalid table name [" + freeHandTableName + "]");
        }

        List<TableMetadata> exactTableNameCandidates = new ArrayList<>();
        List<TableMetadata> wrongCaseCandidates = new ArrayList<>();
        try {
            List<TableMetadata> tableMetadatas = metadataManager.getTables(null);
            for (TableMetadata tableMetadata : tableMetadatas) {
                if (parts[0].equals(tableMetadata.tableName)) {
                    if (schemaMatches(parts, tableMetadata.schemaMetadata)) {
                        exactTableNameCandidates.add(tableMetadata);
                    }
                } else if (parts[0].equalsIgnoreCase(tableMetadata.tableName)) {
                    if (schemaMatches(parts, tableMetadata.schemaMetadata)) {
                        wrongCaseCandidates.add(tableMetadata);
                    }
                }
            }

            List<TableMetadata> candidates = exactTableNameCandidates.isEmpty() ? wrongCaseCandidates
                    : exactTableNameCandidates;
            if (candidates.isEmpty()) {
                throw new NoSuchTableException(freeHandTableName);
            }

            if (candidates.size() == 1) {
                TableMetadata tableMetadata = candidates.get(0);
                metadataManager.loadColumns(tableMetadata);
                return tableMetadata;
            }

            throw new AmbiguousTableNameException(freeHandTableName); // add message about multiple candidates.
        } catch (SQLException exc) {
            throw new DataSetException("Exception while retrieving Table Metadata", exc);
        }
    }

    boolean schemaMatches(String[] parts, SchemaMetadata schemaMetadata) {
        if (parts.length == 1) {
            return true;
        }
        if (parts.length == 3) {
            return parts[1].equalsIgnoreCase(schemaMetadata.schema)
                    && parts[2].equalsIgnoreCase(schemaMetadata.catalog);
        }
        // parts.length = 2
        if (schemaMetadata.schema != null) {
            return parts[1].equalsIgnoreCase(schemaMetadata.schema);
        }
        if (schemaMetadata.catalog != null) {
            return parts[1].equalsIgnoreCase(schemaMetadata.catalog);
        }
        return false;
    }
}
