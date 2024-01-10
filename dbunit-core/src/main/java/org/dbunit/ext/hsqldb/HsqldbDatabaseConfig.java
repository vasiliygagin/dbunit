/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.ext.hsqldb;

/**
 *
 */
public class HsqldbDatabaseConfig extends io.github.vasiliygagin.dbunit.jdbc.DatabaseConfig {

    public HsqldbDatabaseConfig() {
        setDataTypeFactory(new HsqldbDataTypeFactory());
    }
}