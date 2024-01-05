/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit;

import org.dbunit.ext.mysql.MySqlDataTypeFactory;

import io.github.vasiliygagin.dbunit.jdbc.DatabaseConfig;

/**
 *
 */
public class MySqlDatabaseConfig extends DatabaseConfig {

    public MySqlDatabaseConfig() {
        setDataTypeFactory(new MySqlDataTypeFactory());
    }
}
