/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.junit.internal.connections;

import org.dbunit.database.AbstractDatabaseConnection;
import org.dbunit.junit.ConnectionSource;

/**
 *
 */
public class SingleConnectionSource implements ConnectionSource {

    private final AbstractDatabaseConnection connection;

    public SingleConnectionSource(AbstractDatabaseConnection connection) {
        this.connection = connection;
    }

    @Override
    public AbstractDatabaseConnection getConnection() {
        return connection;
    }

    @Override
    public void releaseConnection(AbstractDatabaseConnection connection) {
        // doing nothing since connection is reused in multiple tests
    }
}
