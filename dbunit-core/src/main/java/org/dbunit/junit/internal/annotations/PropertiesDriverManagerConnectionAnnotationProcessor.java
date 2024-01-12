/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.junit.internal.annotations;

import java.sql.Connection;

import javax.sql.DataSource;

import org.dbunit.internal.connections.DriverManagerConnectionsFactory;
import org.dbunit.internal.connections.SingleConnectionDataSource;
import org.dbunit.junit.ConnectionSource;
import org.dbunit.junit.PropertiesDriverManagerConnection;
import org.dbunit.junit.internal.GlobalContext;
import org.dbunit.junit.internal.TestContext;
import org.dbunit.junit.internal.connections.DatabaseConnectionManager;

/**
 *
 */
class PropertiesDriverManagerConnectionAnnotationProcessor {

    private static final GlobalContext context = GlobalContext.getIt();
    private static final DriverManagerConnectionsFactory driverManagerConnectionsFactory = DriverManagerConnectionsFactory
            .getIT();

    public void process(Class<? extends Object> klass, TestContext testContext) {
        DatabaseConnectionManager dbConnectionManager = context.getDbConnectionManager();

        PropertiesDriverManagerConnection annotation = klass.getAnnotation(PropertiesDriverManagerConnection.class);
        if (annotation != null) {
            Connection jdbcConnection = driverManagerConnectionsFactory.buildConnection(
                    System.getProperty(PropertiesDriverManagerConnection.DBUNIT_DRIVER_CLASS),
                    System.getProperty(PropertiesDriverManagerConnection.DBUNIT_CONNECTION_URL),
                    System.getProperty(PropertiesDriverManagerConnection.DBUNIT_USERNAME),
                    System.getProperty(PropertiesDriverManagerConnection.DBUNIT_PASSWORD));
            DataSource dataSource = new SingleConnectionDataSource(jdbcConnection);
            ConnectionSource connectionSource = dbConnectionManager.registerDataSourceInstance(dataSource);
            testContext.addConnecionSource(annotation.name(), connectionSource);
        }
    }
}
