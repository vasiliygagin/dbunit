/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit;

import org.dbunit.ext.postgresql.PostgresqlDataTypeFactory;

import io.github.vasiliygagin.dbunit.jdbc.DatabaseConfig;

/**
 *
 */
public final class PostgresqlDatabaseConfig extends DatabaseConfig {

    public PostgresqlDatabaseConfig() {
        setDataTypeFactory(new PostgresqlDataTypeFactory());
    }
}