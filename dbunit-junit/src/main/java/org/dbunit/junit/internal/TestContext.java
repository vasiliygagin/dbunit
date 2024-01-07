/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.junit.internal;

import java.util.HashMap;
import java.util.Map;

import org.dbunit.database.DatabaseConnection;
import org.dbunit.junit.ConnectionSource;
import org.dbunit.junit.DatabaseException;

/**
 *
 */
public class TestContext {

    private Map<String, ConnectionSource> connectionSources = new HashMap<>();
    private DatabaseConnection connection;
    private String schema;

    public DatabaseConnection getConnection() {
        return connection;
    }

    public void setConnection(DatabaseConnection connection) {
        this.connection = connection;
    }

    /**
     * @param dataSourceName
     * @param connectionSource
     */
    public void addConnecionSource(String dataSourceName, ConnectionSource connectionSource) {
        connectionSources.put(dataSourceName, connectionSource);

    }

    public void addConnecionSource(ConnectionSource connectionSource) {
        connectionSources.put("", connectionSource);
    }

    public ConnectionSource getSingleConnectionSource() {
        if (connectionSources.size() > 1) {
            throw new AssertionError("Multiple connections registered for test case");
        }
        if (connectionSources.size() == 1) {
            return connectionSources.values().iterator().next();
        }
        throw new AssertionError("No connections registered for test case");
    }

    public DatabaseConnection getSingleSourceConnection() throws DatabaseException {
        if (connection == null) {
            connection = getSingleConnectionSource().getConnection();
        }
        return connection;
    }

    public void releaseConnection() {
        if (connection != null) {
            getSingleConnectionSource().releaseConnection(connection);
            this.connection = null;
        }
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }
}
