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
import java.util.HashSet;
import java.util.Set;

import org.dbunit.DatabaseUnitRuntimeException;

/**
 *
 */
public class SchemasManager {

    final Connection jdbcConnectione;
    // TODO: this needs to be coming from DB config
    private Set<SchemaMetadata> systemSchemas = new HashSet<>(
            asList(new SchemaMetadata(null, "INFORMATION_SCHEMA"), new SchemaMetadata(null, "SYSTEM_LOBS")));
    private Set<SchemaMetadata> allSchemas;

    private static enum NameCase {
        UPPER, LOWER, MIXED;
    }

    private NameCase schemaNamesCase;

    /**
     * @param jdbcConnectione
     */
    public SchemasManager(Connection jdbcConnectione) {
        this.jdbcConnectione = jdbcConnectione;
    }

    public Set<SchemaMetadata> getAllSchemas() {
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
            schemaNamesCase = calculateSchemaNameCase();
        }
        return allSchemas;
    }

    /**
     * @return
     */
    private NameCase calculateSchemaNameCase() {
        if (allSchemas.isEmpty()) {
            return NameCase.MIXED;
        }
        NameCase previousCase = null;

        for (SchemaMetadata schemaMetadata : allSchemas) {
            String schema = schemaMetadata.schema;
            if (schema != null) {
                NameCase nc = getCase(schema);
                if (previousCase != nc) {
                    if (previousCase == null) {
                        previousCase = nc;
                    } else {
                        return NameCase.MIXED;
                    }
                }
            }

            String catalog = schemaMetadata.catalog;
            if (catalog != null) {
                NameCase nc = getCase(catalog);
                if (previousCase != nc) {
                    if (previousCase == null) {
                        previousCase = nc;
                    } else {
                        return NameCase.MIXED;
                    }
                }
            }
        }
        return previousCase;
    }

    private static NameCase getCase(String name) {
        if (name.equals(name.toUpperCase())) {
            return NameCase.UPPER;
        } else if (name.equals(name.toLowerCase())) {
            return NameCase.LOWER;
        } else {
            return NameCase.MIXED;
        }
    }

    private String adjustName(String name) {
        if (name == null) {
            return null;
        }
        if (schemaNamesCase == NameCase.UPPER) {
            return name.toUpperCase();
        }
        if (schemaNamesCase == NameCase.LOWER) {
            return name.toLowerCase();
        }
        return name;
    }

    public SchemaMetadata findSchema(String catalog, String schema) {
        catalog = adjustName(catalog);
        schema = adjustName(schema);
        SchemaMetadata candidate = new SchemaMetadata(catalog, schema);

        for (SchemaMetadata schemaMetadata : getAllSchemas()) {
            if (candidate.equals(schemaMetadata)) {
                return schemaMetadata;
            }
        }

        throw new DatabaseUnitRuntimeException("Unable to find schema=[" + schema + "], catalog=[" + catalog + "]");
    }

    /**
     * @param schema
     * @return
     */
    public SchemaMetadata findSchema(String schema) {
        schema = adjustName(schema);

        for (SchemaMetadata schemaMetadata : getAllSchemas()) {
            if (schema.equals(schemaMetadata.schema)) {
                return schemaMetadata;
            }
        }

        for (SchemaMetadata schemaMetadata : getAllSchemas()) {
            if (schema.equals(schemaMetadata.catalog)) {
                return schemaMetadata;
            }
        }

        throw new DatabaseUnitRuntimeException("Unable to find schema=[" + schema + "]");
    }
}
