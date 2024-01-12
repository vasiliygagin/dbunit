/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.junit.internal.annotations;

import java.sql.Connection;

import javax.sql.DataSource;

import org.dbunit.internal.connections.DriverManagerConnectionsFactory;
import org.dbunit.internal.connections.SingleConnectionDataSource;
import org.dbunit.junit.ConnectionSource;
import org.dbunit.junit.DriverManagerConnection;
import org.dbunit.junit.internal.TestContext;
import org.dbunit.junit.internal.connections.DataSourceConnectionSource;

/**
 *
 */
class DriverManagerConnectionAnnotationProcessor {

    private static final DriverManagerConnectionsFactory driverManagerConnectionsFactory = DriverManagerConnectionsFactory
            .getIT();

    /**
     * @param klass
     * @param testContext TODO
     */
    public void process(Class<? extends Object> klass, TestContext testContext) {

        DriverManagerConnection[] annotations = klass.getAnnotationsByType(DriverManagerConnection.class);
        for (DriverManagerConnection annotation : annotations) {
            Connection jdbcConnection = driverManagerConnectionsFactory.buildConnection(annotation.driver(),
                    annotation.url(), annotation.user(), annotation.password());
            DataSource dataSource = new SingleConnectionDataSource(jdbcConnection);
            ConnectionSource connectionSource = new DataSourceConnectionSource(dataSource);
            testContext.addConnecionSource(annotation.name(), connectionSource);
        }
    }
}
