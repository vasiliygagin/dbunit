/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit;

import org.dbunit.ext.oracle.OracleDataTypeFactory;

import io.github.vasiliygagin.dbunit.jdbc.DatabaseConfig;

/**
 *
 */
public class OracleDatabaseConfig extends DatabaseConfig {

    public OracleDatabaseConfig() {
        setDataTypeFactory(new OracleDataTypeFactory());
    }
}