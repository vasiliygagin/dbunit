/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.junit.internal;

/**
 *
 */
public class SqlScript {

    public final String dataSourceName;
    public final String filePath;

    /**
     * @param dataSourceName
     * @param filePath
     */
    public SqlScript(String dataSourceName, String filePath) {
        this.dataSourceName = dataSourceName;
        this.filePath = filePath;
    }
}
