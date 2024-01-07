/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.junit.internal.annotations;

import org.dbunit.junit.ConnectionSource;
import org.dbunit.junit.DataSource;
import org.dbunit.junit.DatabaseException;
import org.dbunit.junit.internal.GlobalContext;
import org.dbunit.junit.internal.TestContext;
import org.dbunit.junit.internal.connections.DatabaseConnectionManager;

/**
 *
 */
public class DataSourceAnnotationProcessor {

    private static final GlobalContext context = GlobalContext.getIt();

    /**
     * @param klass
     * @param testContext
     * @throws DatabaseException
     */
    public void process(Class<? extends Object> klass, TestContext testContext) throws DatabaseException {
        DatabaseConnectionManager dbConnectionManager = context.getDbConnectionManager();

        DataSource[] annotations = klass.getAnnotationsByType(DataSource.class);
        for (DataSource annotation : annotations) {
            String dataSourceName = annotation.name();
            Class<? extends javax.sql.DataSource> dataSourceClass = annotation.dataSource();
            javax.sql.DataSource dataSource = buildDataSource(dataSourceClass);
            ConnectionSource connectionSource = dbConnectionManager.registerDataSource(dataSource);
            testContext.addConnecionSource(dataSourceName, connectionSource);
        }
    }

    private javax.sql.DataSource buildDataSource(Class<? extends javax.sql.DataSource> dataSourceClass)
            throws DatabaseException {
        try {
            return dataSourceClass.newInstance();
        } catch (InstantiationException | IllegalAccessException exc) {
            throw new DatabaseException(exc);
        }
    }
}
