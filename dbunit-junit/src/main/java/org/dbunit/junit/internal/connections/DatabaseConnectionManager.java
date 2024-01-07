/*
 * Copyright (C)2024, Vasiliy Gagin. All rights reserved.
 */
package org.dbunit.junit.internal.connections;

import java.util.HashMap;
import java.util.Map;

import javax.sql.DataSource;

import org.dbunit.database.DatabaseConnection;
import org.dbunit.junit.ConnectionSource;
import org.dbunit.junit.DatabaseException;

/**
 * Manages multiple connections to multiple databases.
 * Work in progress.
 */
public class DatabaseConnectionManager {

    private Map<ConnectionKey, ConnectionSource> registeredConnectionSources = new HashMap<>();
    private ConnectionSource connectionSource = null;

    public DatabaseConnection getConnection() throws DatabaseException {
        return connectionSource.getConnection();
    }

    /**
     * @param connection
     */
    public void releaseConnection(DatabaseConnection connection) {
        connectionSource.releaseConnection(connection);
    }

    /**
     * @param driver
     * @param url
     * @param user
     * @param password
     * @return
     */
    public ConnectionSource fetchDriverManagerConnection(String driver, String url, String user, String password) {
        DriverManagerConnectionKey key = new DriverManagerConnectionKey(driver, url, user, password);
        ConnectionSource connectionSource = registeredConnectionSources.get(key);
        if (connectionSource == null) {
            connectionSource = new DriverManagerConnectionSource(driver, url, user, password);
            registeredConnectionSources.put(key, connectionSource);
        }
        return connectionSource;
    }

    /**
     * @param dataSource
     * @return
     */
    public ConnectionSource registerDataSource(DataSource dataSource) {
        DataSourceKey key = new DataSourceKey(dataSource.getClass());
        ConnectionSource connectionSource = registeredConnectionSources.get(key);
        if (connectionSource == null) {
            connectionSource = new DataSourceConnectionSource(dataSource);
            registeredConnectionSources.put(key, connectionSource);
        }
        return connectionSource;
    }
}
