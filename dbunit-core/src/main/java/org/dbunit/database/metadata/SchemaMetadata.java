/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.database.metadata;

import java.util.Objects;

/**
 * Stores Jdbc Schema id
 */
public class SchemaMetadata {

    public final String catalog;
    public final String schema;

    public SchemaMetadata(String catalog, String schema) {
        this.catalog = emptyToNull(catalog);
        this.schema = emptyToNull(schema);
    }

    private static String emptyToNull(String name) {
        if (name == null || name.isEmpty()) {
            return null;
        }
        return name;
    }

    @Override
    public int hashCode() {
        return Objects.hash(catalog, schema);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        SchemaMetadata other = (SchemaMetadata) obj;
        return Objects.equals(catalog, other.catalog) && Objects.equals(schema, other.schema);
    }
}
