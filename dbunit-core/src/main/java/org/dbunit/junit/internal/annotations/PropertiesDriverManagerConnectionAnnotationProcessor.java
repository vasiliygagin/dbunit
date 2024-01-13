/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.junit.internal.annotations;

import java.sql.Connection;

import javax.sql.DataSource;

import org.dbunit.internal.connections.DriverManagerConnectionSource;
import org.dbunit.internal.connections.SingleConnectionDataSource;
import org.dbunit.internal.connections.UncloseableConnection;
import org.dbunit.junit.ConnectionSource;
import org.dbunit.junit.DatabaseException;
import org.dbunit.junit.PropertiesDriverManagerConnection;
import org.dbunit.junit.internal.GlobalContext;
import org.dbunit.junit.internal.TestContext;
import org.dbunit.junit.internal.connections.DataSourceConnectionSource;

/**
 *
 */
class PropertiesDriverManagerConnectionAnnotationProcessor {

    private static GlobalContext context = GlobalContext.getIt();

    public void process(Class<? extends Object> klass, TestContext testContext) throws DatabaseException {
        DriverManagerConnectionSource driverManagerConnectionSource = context.getDriverManagerConnectionSource();

        PropertiesDriverManagerConnection annotation = klass.getAnnotation(PropertiesDriverManagerConnection.class);
        if (annotation != null) {
            Connection jdbcConnection = driverManagerConnectionSource.fetchConnection(
                    System.getProperty(PropertiesDriverManagerConnection.DBUNIT_DRIVER_CLASS),
                    System.getProperty(PropertiesDriverManagerConnection.DBUNIT_CONNECTION_URL),
                    System.getProperty(PropertiesDriverManagerConnection.DBUNIT_USERNAME),
                    System.getProperty(PropertiesDriverManagerConnection.DBUNIT_PASSWORD));
            UncloseableConnection uncloseableConnection = new UncloseableConnection(jdbcConnection);
            DataSource dataSource = new SingleConnectionDataSource(uncloseableConnection);
            ConnectionSource connectionSource = new DataSourceConnectionSource(dataSource);
            testContext.addConnecionSource(annotation.name(), connectionSource);
        }
    }
}
