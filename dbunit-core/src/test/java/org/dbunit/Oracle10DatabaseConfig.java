/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit;

import org.dbunit.ext.oracle.Oracle10DataTypeFactory;

import io.github.vasiliygagin.dbunit.jdbc.DatabaseConfig;

/**
 *
 */
public final class Oracle10DatabaseConfig extends DatabaseConfig {
    {
        setDataTypeFactory(new Oracle10DataTypeFactory());
    }
}