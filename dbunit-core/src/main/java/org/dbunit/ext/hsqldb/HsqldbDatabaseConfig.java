/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.ext.hsqldb;

import org.dbunit.database.DatabaseConfig;

/**
 *
 */
public class HsqldbDatabaseConfig extends DatabaseConfig {

    public HsqldbDatabaseConfig() {
        setDataTypeFactory(new HsqldbDataTypeFactory());
    }
}