/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.junit.internal.annotations;

import org.dbunit.junit.ConnectionSource;
import org.dbunit.junit.DriverManagerConnection;
import org.dbunit.junit.internal.GlobalContext;
import org.dbunit.junit.internal.TestContext;
import org.dbunit.junit.internal.connections.DatabaseConnectionManager;

/**
 *
 */
class DriverManagerConnectionAnnotationProcessor {

    private static final GlobalContext context = GlobalContext.getIt();

    /**
     * @param klass
     * @param testContext TODO
     */
    public void process(Class<? extends Object> klass, TestContext testContext) {
        DatabaseConnectionManager dbConnectionManager = context.getDbConnectionManager();

        DriverManagerConnection[] annotations = klass.getAnnotationsByType(DriverManagerConnection.class);
        for (DriverManagerConnection annotation : annotations) {
            ConnectionSource connectionSource = dbConnectionManager.fetchDriverManagerConnection(annotation.driver(),
                    annotation.url(), annotation.user(), annotation.password());
            testContext.addConnecionSource(connectionSource);
        }
    }

}
