/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package io.github.vasiliygagin.dbunit.jdbc;

import javax.sql.DataSource;

/**
 *Responsible for loading and serving database metadata
 */
public class MetadataManager {

    final DataSource dataSource;
    final String defaultCatalog;
    final String defaultSchema;

    /**
     * @param dataSource to use for accessing metadata
     * @param defaultSchema
     * @param defaultCatalog
     */
    public MetadataManager(DataSource dataSource, String defaultCatalog, String defaultSchema) {
        this.dataSource = dataSource;
        this.defaultCatalog = defaultCatalog;
        this.defaultSchema = defaultSchema;
    }

}
