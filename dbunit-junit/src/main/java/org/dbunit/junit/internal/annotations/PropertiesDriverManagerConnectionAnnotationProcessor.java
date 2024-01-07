/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.junit.internal.annotations;

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

    public void process(Class<? extends Object> klass, TestContext testContext) {
        DatabaseConnectionManager dbConnectionManager = context.getDbConnectionManager();

        PropertiesDriverManagerConnection annotation = klass.getAnnotation(PropertiesDriverManagerConnection.class);
        if (annotation != null) {
            ConnectionSource connectionSource = dbConnectionManager.fetchDriverManagerConnection(
                    System.getProperty(PropertiesDriverManagerConnection.DBUNIT_DRIVER_CLASS),
                    System.getProperty(PropertiesDriverManagerConnection.DBUNIT_CONNECTION_URL),
                    System.getProperty(PropertiesDriverManagerConnection.DBUNIT_USERNAME),
                    System.getProperty(PropertiesDriverManagerConnection.DBUNIT_PASSWORD));
            testContext.addConnecionSource(connectionSource);
        }
    }
}
