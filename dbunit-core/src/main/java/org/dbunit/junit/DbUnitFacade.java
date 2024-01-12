/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.junit;

import java.sql.Connection;

import org.dbunit.database.DatabaseConnection;
import org.dbunit.junit.internal.DbUnitRule;

/**
 * JUnit 4 way of integrating DbUnit framework into JUnit
 *
 * <pre>
 * </pre>
 */
public class DbUnitFacade extends DbUnitRule {

    /**
     * @return a connection to default database
     * @throws DatabaseException
     */
    public DatabaseConnection getConnection() throws DatabaseException {
        return getTestContext().getConnection();
    }

    @Override
    protected void after() {
        rollbackConnections();
        releaseConnections();
        super.after();
    }

    protected void rollbackConnections() {
        getTestContext().rollbackConnections();
    }

    protected void releaseConnections() {
        getTestContext().releaseConnections();
    }

    public Connection getJdbcConnection() throws Exception {
        return getConnection().getConnection();
    }
}
