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
import org.dbunit.junit.DriverManagerConnection;
import org.dbunit.junit.internal.GlobalContext;
import org.dbunit.junit.internal.TestContext;
import org.dbunit.junit.internal.connections.DataSourceConnectionSource;

/**
 *
 */
class DriverManagerConnectionAnnotationProcessor {

    private static GlobalContext context = GlobalContext.getIt();

    /**
     * @param klass
     * @param testContext TODO
     * @throws DatabaseException
     */
    public void process(Class<? extends Object> klass, TestContext testContext) throws DatabaseException {
        DriverManagerConnectionSource driverManagerConnectionSource = context.getDriverManagerConnectionSource();

        DriverManagerConnection[] annotations = klass.getAnnotationsByType(DriverManagerConnection.class);
        for (DriverManagerConnection annotation : annotations) {
            Connection jdbcConnection = driverManagerConnectionSource.fetchConnection(annotation.driver(),
                    annotation.url(), annotation.user(), annotation.password());
            UncloseableConnection uncloseableConnection = new UncloseableConnection(jdbcConnection);
            DataSource dataSource = new SingleConnectionDataSource(uncloseableConnection);
            ConnectionSource connectionSource = new DataSourceConnectionSource(dataSource);
            testContext.addConnecionSource(annotation.name(), connectionSource);
        }
    }
}
