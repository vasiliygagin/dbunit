/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.junit.internal;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.dbunit.database.DatabaseConnection;
import org.dbunit.junit.ConnectionSource;
import org.dbunit.junit.DatabaseException;

/**
 *
 */
public class TestContext {

    private Map<String, ConnectionSource> connectionSources = new HashMap<>();
    private Map<String, DatabaseConnection> connections = new HashMap<>();
    private String schema;

    public DatabaseConnection getConnection() throws DatabaseException {
        String connectionName = getConnectionName();
        return getConnection(connectionName);
    }

    /**
     * @param connectionName
     * @return
     * @throws DatabaseException
     */
    public DatabaseConnection getConnection(String connectionName) throws DatabaseException {
        DatabaseConnection databaseConnection = connections.get(connectionName);
        if (databaseConnection == null) {
            ConnectionSource connectionSource = connectionSources.get(connectionName);
            if (connectionSource == null) {
                throw new AssertionError("No connection named [" + connectionName + "] is registered for test case");
            }
            databaseConnection = connectionSource.getConnection();
            connections.put(connectionName, databaseConnection);
        }
        return databaseConnection;
    }

    public String getConnectionName() {
        if (connectionSources.size() > 1) {
            throw new AssertionError("Multiple connections registered for test case");
        }
        if (connectionSources.size() == 1) {
            return connectionSources.keySet().iterator().next();
        }
        throw new AssertionError("No connections registered for test case");
    }

    /**
     * @param dataSourceName
     * @param connectionSource
     */
    public void addConnecionSource(String dataSourceName, ConnectionSource connectionSource) {
        if (connectionSources.containsKey(dataSourceName)) {
            throw new IllegalStateException("Datasource with name [" + dataSourceName + "] is already registered");
        }
        connectionSources.put(dataSourceName, connectionSource);

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

    public void rollbackConnections() {
        for (DatabaseConnection connection : connections.values()) {
            connection.rollback();
        }
    }

    public void releaseConnections() {
        for (Entry<String, DatabaseConnection> entry : connections.entrySet()) {
            String connectionName = entry.getKey();
            DatabaseConnection connection = entry.getValue();
            connectionSources.get(connectionName).releaseConnection(connection);
        }
        connections.clear();
    }

    public void shutdownConnections() {
        for (DatabaseConnection connection : connections.values()) {
            connection.shutdown();
        }
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }
}
