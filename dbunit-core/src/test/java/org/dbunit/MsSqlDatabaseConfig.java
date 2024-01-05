/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit;

import org.dbunit.ext.mssql.MsSqlDataTypeFactory;

import io.github.vasiliygagin.dbunit.jdbc.DatabaseConfig;

/**
 *
 */
public class MsSqlDatabaseConfig extends DatabaseConfig {

    public MsSqlDatabaseConfig() {
        setDataTypeFactory(new MsSqlDataTypeFactory());
    }
}
